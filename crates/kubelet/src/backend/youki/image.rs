//! Image references and pulls on the `oci-image` crate.
//!
//! Layers are pulled anonymously over the kubelet's existing reqwest client
//! into a content-addressed store (every blob checked against its sha256
//! digest) and unpacked once each, in one pass, into `layers/<diff_id>/` in
//! overlayfs format. A container's rootfs is an overlay of its image's layer
//! directories; an updated image fetches and unpacks only its new layers.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub use oci_image::ImageRef;
use oci_image::{LayerStore, Layout, OverlayXattr, UnpackOptions};

use crate::backend::ImageConfig;

/// What the store keeps per pulled image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredImage {
    /// `sha256:<config digest>`, like Docker's image ID.
    pub id: String,
    pub reference: String,
    pub config: StoredConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoredConfig {
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
}

impl From<StoredConfig> for ImageConfig {
    fn from(c: StoredConfig) -> Self {
        ImageConfig {
            entrypoint: c.entrypoint,
            cmd: c.cmd,
            env: c.env,
            user: c.user,
            working_dir: c.working_dir,
        }
    }
}

/// Images on disk: `<root>/images/<hex>/{rootfs,image.json}` plus
/// `<root>/refs/<reference>.json` pointing at one of them.
#[derive(Debug, Clone)]
pub struct ImageStore {
    root: PathBuf,
}

impl ImageStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn ref_file(&self, r: &ImageRef) -> PathBuf {
        let name: String = r
            .canonical()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.root.join("refs").join(format!("{}.json", name))
    }

    fn image_dir(&self, id: &str) -> PathBuf {
        self.root
            .join("images")
            .join(id.trim_start_matches("sha256:"))
    }

    fn layer_store(&self) -> Result<LayerStore> {
        Ok(LayerStore::open(&self.root.join("layers"))?)
    }

    /// The layer directories of image `id`, bottom first.
    pub fn layers(&self, id: &str) -> Result<Vec<PathBuf>> {
        Ok(self.layer_store()?.image_layers(&id.parse()?)?)
    }

    /// Look an image up by reference (or by `sha256:` ID).
    pub fn get(&self, image: &str) -> Result<StoredImage> {
        let path = if image.starts_with("sha256:") {
            self.image_dir(image).join("image.json")
        } else {
            let r = ImageRef::parse(image)?;
            let link: StoredImage = read_json(&self.ref_file(&r))
                .with_context(|| format!("No such image: {}", image))?;
            self.image_dir(&link.id).join("image.json")
        };
        read_json(&path).with_context(|| format!("No such image: {}", image))
    }

    /// Pull `image`, calling `progress` with status lines.
    pub async fn pull(
        &self,
        client: &reqwest::Client,
        image: &str,
        progress: &mut (dyn FnMut(String) + Send),
    ) -> Result<StoredImage> {
        let layers = self.layer_store()?;
        let pulled = oci_image::pull::pull(client, image, &layers, progress).await?;
        let r = pulled.reference.clone();
        let id = pulled.id.to_string();
        let dir = self.image_dir(&id);

        let (image_id, image_layers) = (pulled.id.clone(), pulled.layers.clone());
        tokio::task::spawn_blocking(move || {
            let opts = UnpackOptions {
                layout: Layout::Overlay(OverlayXattr::Trusted),
                ..UnpackOptions::default()
            };
            layers.add_image(&image_id, &image_layers, &opts)?;
            // The unpacked layers are what containers use; drop their blobs.
            layers.prune_unpacked_blobs()
        })
        .await??;
        if !dir.join("image.json").exists() {
            let tmp = self
                .root
                .join("tmp")
                .join(uuid::Uuid::new_v4().simple().to_string());
            fs::create_dir_all(&tmp)?;
            let stored = StoredImage {
                id: id.clone(),
                reference: r.canonical(),
                config: stored_config(&pulled.config),
            };
            write_json(&tmp.join("image.json"), &stored)?;
            fs::create_dir_all(dir.parent().unwrap())?;
            if fs::rename(&tmp, &dir).is_err() {
                // Another pull of the same image won the race.
                let _ = fs::remove_dir_all(&tmp);
            }
        }

        let stored: StoredImage = read_json(&dir.join("image.json"))?;
        let link = StoredImage {
            reference: r.canonical(),
            ..stored.clone()
        };
        let ref_file = self.ref_file(&r);
        fs::create_dir_all(ref_file.parent().unwrap())?;
        write_json(&ref_file, &link)?;
        progress(format!("Pulled {} ({})", r.canonical(), id));
        Ok(stored)
    }
}

fn stored_config(config: &serde_json::Value) -> StoredConfig {
    let c = &config["config"];
    let strings = |v: &serde_json::Value| -> Option<Vec<String>> {
        v.as_array().map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
    };
    let string = |v: &serde_json::Value| v.as_str().filter(|s| !s.is_empty()).map(str::to_string);
    StoredConfig {
        entrypoint: strings(&c["Entrypoint"]),
        cmd: strings(&c["Cmd"]),
        env: strings(&c["Env"]),
        user: string(&c["User"]),
        working_dir: string(&c["WorkingDir"]),
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

/// Write JSON atomically (temp file + rename).
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> (String, String, String) {
        let r = ImageRef::parse(s).unwrap();
        (r.registry, r.repository, r.reference)
    }

    #[test]
    fn parses_references_like_docker() {
        let s = |a: &str, b: &str, c: &str| (a.to_string(), b.to_string(), c.to_string());
        assert_eq!(r("busybox"), s("docker.io", "library/busybox", "latest"));
        assert_eq!(r("busybox:1.36"), s("docker.io", "library/busybox", "1.36"));
        assert_eq!(
            r("ghcr.io/org/app@sha256:abc"),
            s("ghcr.io", "org/app", "sha256:abc")
        );
        assert_eq!(
            ImageRef::parse("busybox").unwrap().canonical(),
            "docker.io/library/busybox:latest"
        );
    }

    /// Network: `cargo test --features youki -- --ignored pulls_busybox`.
    #[tokio::test]
    #[ignore]
    async fn pulls_busybox_from_docker_hub() {
        let root = tempfile::tempdir().unwrap();
        let store = ImageStore::new(root.path().to_path_buf());
        let client = reqwest::Client::new();
        let mut lines = Vec::new();
        let img = store
            .pull(&client, "busybox:1.36", &mut |l| lines.push(l))
            .await
            .unwrap();
        let layers = store.layers(&img.id).unwrap();
        assert!(layers.iter().any(|l| l.join("bin/sh").exists()));
        assert_eq!(store.get("busybox:1.36").unwrap().id, img.id);
        // A second pull finds every blob and the rootfs already there.
        let again = store
            .pull(&client, "busybox:1.36", &mut |_| {})
            .await
            .unwrap();
        assert_eq!(again.id, img.id);
    }
}
