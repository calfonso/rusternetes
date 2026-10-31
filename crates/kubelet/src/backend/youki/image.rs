//! Image references, a small OCI distribution client and layer unpacking.
//!
//! Images are pulled anonymously over the kubelet's existing reqwest client,
//! every blob is checked against its sha256 digest, and the layers are
//! flattened into one read-only rootfs per image. Containers get an overlay
//! on top of it.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::backend::ImageConfig;

const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, \
    application/vnd.oci.image.manifest.v1+json, \
    application/vnd.docker.distribution.manifest.list.v2+json, \
    application/vnd.docker.distribution.manifest.v2+json";

/// A parsed image reference such as `busybox`, `nginx:1.27` or
/// `ghcr.io/org/app@sha256:...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub registry: String,
    pub repository: String,
    /// A tag, or a digest (`sha256:...`).
    pub reference: String,
}

impl ImageRef {
    pub fn parse(image: &str) -> Result<Self> {
        let image = image.trim();
        if image.is_empty() {
            bail!("empty image reference");
        }
        let (name, digest) = match image.split_once('@') {
            Some((n, d)) => (n, Some(d)),
            None => (image, None),
        };
        let (registry, rest) = match name.split_once('/') {
            Some((first, rest))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (first.to_string(), rest.to_string())
            }
            _ => ("docker.io".to_string(), name.to_string()),
        };
        // A tag is a ':' after the last '/'.
        let (repository, tag) = match rest.rfind(':') {
            Some(i) if !rest[i..].contains('/') => {
                (rest[..i].to_string(), rest[i + 1..].to_string())
            }
            _ => (rest, "latest".to_string()),
        };
        let repository = if registry == "docker.io" && !repository.contains('/') {
            format!("library/{}", repository)
        } else {
            repository
        };
        if repository.is_empty() {
            bail!("invalid image reference {:?}", image);
        }
        Ok(Self {
            registry,
            repository,
            reference: digest.map(str::to_string).unwrap_or(tag),
        })
    }

    /// The fully qualified name, e.g. `docker.io/library/busybox:latest`.
    pub fn canonical(&self) -> String {
        if self.reference.contains(':') {
            format!("{}/{}@{}", self.registry, self.repository, self.reference)
        } else {
            format!("{}/{}:{}", self.registry, self.repository, self.reference)
        }
    }

    fn api_base(&self) -> String {
        let host = if self.registry == "docker.io" {
            "registry-1.docker.io"
        } else {
            &self.registry
        };
        let scheme = if host.starts_with("localhost") || host.starts_with("127.0.0.1") {
            "http"
        } else {
            "https"
        };
        format!("{}://{}/v2/{}", scheme, host, self.repository)
    }
}

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

    pub fn rootfs(&self, id: &str) -> PathBuf {
        self.image_dir(id).join("rootfs")
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
        let r = ImageRef::parse(image)?;
        progress(format!("Pulling {}", r.canonical()));
        let mut reg = Registry {
            client,
            image: &r,
            token: None,
        };

        let (mut manifest, mut media_type) = reg.manifest(&r.reference).await?;
        if is_index(&media_type, &manifest) {
            let digest = pick_platform(&manifest)?;
            (manifest, media_type) = reg.manifest(&digest).await?;
        }
        if media_type.contains("v1+prettyjws") || manifest.get("fsLayers").is_some() {
            bail!("unsupported on youki: schema 1 image manifests ({})", image);
        }

        let config_digest = manifest["config"]["digest"]
            .as_str()
            .ok_or_else(|| anyhow!("image manifest has no config"))?
            .to_string();
        let id = config_digest.clone();
        let dir = self.image_dir(&id);

        if !dir.join("image.json").exists() {
            let config_bytes = reg.blob_bytes(&config_digest).await?;
            let config: serde_json::Value = serde_json::from_slice(&config_bytes)?;

            let tmp = self
                .root
                .join("tmp")
                .join(uuid::Uuid::new_v4().simple().to_string());
            let rootfs = tmp.join("rootfs");
            fs::create_dir_all(&rootfs)?;
            let layers = manifest["layers"].as_array().cloned().unwrap_or_default();
            for (i, layer) in layers.iter().enumerate() {
                let digest = layer["digest"]
                    .as_str()
                    .ok_or_else(|| anyhow!("layer without digest"))?;
                let media = layer["mediaType"].as_str().unwrap_or("");
                if media.contains("zstd") {
                    bail!("unsupported on youki: zstd layers ({})", digest);
                }
                let blob = tmp.join("layer.tar");
                reg.blob_to_file(digest, &blob).await?;
                progress(format!("Layer {}/{} {}", i + 1, layers.len(), digest));
                let (rootfs2, blob2) = (rootfs.clone(), blob.clone());
                tokio::task::spawn_blocking(move || apply_layer(&blob2, &rootfs2)).await??;
                fs::remove_file(&blob)?;
            }

            let stored = StoredImage {
                id: id.clone(),
                reference: r.canonical(),
                config: stored_config(&config),
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

fn is_index(media_type: &str, manifest: &serde_json::Value) -> bool {
    media_type.contains("index")
        || media_type.contains("manifest.list")
        || manifest.get("manifests").is_some()
}

/// Pick the manifest for this machine from an image index.
fn pick_platform(index: &serde_json::Value) -> Result<String> {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        "s390x" => "s390x",
        "riscv64" => "riscv64",
        other => other,
    };
    index["manifests"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|m| m["platform"]["os"] == "linux" && m["platform"]["architecture"] == arch)
        .and_then(|m| m["digest"].as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no linux/{} image in the index", arch))
}

struct Registry<'a> {
    client: &'a reqwest::Client,
    image: &'a ImageRef,
    token: Option<String>,
}

impl Registry<'_> {
    async fn get(&mut self, url: &str, accept: &str) -> Result<reqwest::Response> {
        for _ in 0..2 {
            let mut req = self.client.get(url).header("Accept", accept);
            if let Some(t) = &self.token {
                req = req.bearer_auth(t);
            }
            let resp = req.send().await?;
            if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.token.is_none() {
                let challenge = resp
                    .headers()
                    .get("www-authenticate")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                self.token = Some(self.fetch_token(&challenge).await?);
                continue;
            }
            if !resp.status().is_success() {
                bail!("registry returned {} for {}", resp.status(), url);
            }
            return Ok(resp);
        }
        bail!("registry denied access to {}", url)
    }

    /// Anonymous bearer token for `WWW-Authenticate: Bearer realm=...`.
    async fn fetch_token(&self, challenge: &str) -> Result<String> {
        let params = parse_challenge(challenge)
            .ok_or_else(|| anyhow!("unsupported registry auth challenge: {}", challenge))?;
        let realm = params
            .get("realm")
            .ok_or_else(|| anyhow!("auth challenge has no realm"))?;
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(s) = params.get("service") {
            query.push(("service", s.clone()));
        }
        query.push((
            "scope",
            params
                .get("scope")
                .cloned()
                .unwrap_or_else(|| format!("repository:{}:pull", self.image.repository)),
        ));
        let url = reqwest::Url::parse_with_params(realm, &query)?;
        let body: serde_json::Value = serde_json::from_slice(
            &self
                .client
                .get(url)
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?,
        )?;
        body["token"]
            .as_str()
            .or_else(|| body["access_token"].as_str())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("token endpoint returned no token"))
    }

    async fn manifest(&mut self, reference: &str) -> Result<(serde_json::Value, String)> {
        let url = format!("{}/manifests/{}", self.image.api_base(), reference);
        let resp = self.get(&url, MANIFEST_ACCEPT).await?;
        let media_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = resp.bytes().await?;
        if reference.starts_with("sha256:") {
            verify(reference, &Sha256::digest(&bytes))?;
        }
        Ok((serde_json::from_slice(&bytes)?, media_type))
    }

    async fn blob_bytes(&mut self, digest: &str) -> Result<Vec<u8>> {
        let url = format!("{}/blobs/{}", self.image.api_base(), digest);
        let bytes = self.get(&url, "*/*").await?.bytes().await?.to_vec();
        verify(digest, &Sha256::digest(&bytes))?;
        Ok(bytes)
    }

    async fn blob_to_file(&mut self, digest: &str, path: &Path) -> Result<()> {
        let url = format!("{}/blobs/{}", self.image.api_base(), digest);
        let mut resp = self.get(&url, "*/*").await?;
        let mut file = fs::File::create(path)?;
        let mut hasher = Sha256::new();
        while let Some(chunk) = resp.chunk().await? {
            hasher.update(&chunk);
            file.write_all(&chunk)?;
        }
        verify(digest, &hasher.finalize())
    }
}

fn verify(digest: &str, actual: &[u8]) -> Result<()> {
    let want = digest
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("unsupported digest algorithm: {}", digest))?;
    let got: String = actual.iter().map(|b| format!("{:02x}", b)).collect();
    if got != want {
        bail!("digest mismatch: expected {}, got sha256:{}", digest, got);
    }
    Ok(())
}

/// Parse `Bearer realm="...",service="...",scope="..."`.
fn parse_challenge(header: &str) -> Option<HashMap<String, String>> {
    let rest = header.strip_prefix("Bearer ")?;
    let mut out = HashMap::new();
    let mut s = rest.trim();
    while !s.is_empty() {
        let (key, after) = s.split_once('=')?;
        let after = after.strip_prefix('"')?;
        let end = after.find('"')?;
        out.insert(key.trim().to_string(), after[..end].to_string());
        s = after[end + 1..].trim_start_matches(',').trim();
    }
    Some(out)
}

/// Open a layer blob, gzip-compressed or plain tar.
fn open_layer(blob: &Path) -> Result<Box<dyn Read>> {
    let mut f = fs::File::open(blob)?;
    let mut magic = [0u8; 2];
    let n = f.read(&mut magic)?;
    let f = fs::File::open(blob)?;
    if n == 2 && magic == [0x1f, 0x8b] {
        Ok(Box::new(flate2::read::GzDecoder::new(f)))
    } else {
        Ok(Box::new(f))
    }
}

/// Clean relative path of a tar entry, or `None` if it would escape.
fn entry_path(path: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Normal(p) => out.push(p),
            Component::CurDir | Component::RootDir => {}
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

fn remove_any(path: &Path) {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() => {
            let _ = fs::remove_dir_all(path);
        }
        Ok(_) => {
            let _ = fs::remove_file(path);
        }
        Err(_) => {}
    }
}

/// Apply one OCI layer onto `rootfs`: whiteouts first, then the entries.
pub fn apply_layer(blob: &Path, rootfs: &Path) -> Result<()> {
    // Pass 1: whiteouts remove what lower layers put there.
    let mut archive = tar::Archive::new(open_layer(blob)?);
    for entry in archive.entries()? {
        let entry = entry?;
        let Some(path) = entry_path(&entry.path()?) else {
            continue;
        };
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let parent = path.parent().unwrap_or(Path::new(""));
        if name == ".wh..wh..opq" {
            if let Ok(children) = fs::read_dir(rootfs.join(parent)) {
                for child in children.flatten() {
                    remove_any(&child.path());
                }
            }
        } else if let Some(hidden) = name.strip_prefix(".wh.") {
            remove_any(&rootfs.join(parent).join(hidden));
        }
    }

    // Pass 2: unpack everything else.
    let mut archive = tar::Archive::new(open_layer(blob)?);
    archive.set_preserve_permissions(true);
    archive.set_preserve_ownerships(true);
    archive.set_preserve_mtime(true);
    // File capabilities ride as `security.capability` xattrs (CoreDNS binds
    // :53 as a non-root user through `cap_net_bind_service=+ep` on its
    // binary); without them such a binary gets EACCES at bind.
    archive.set_unpack_xattrs(true);
    archive.set_overwrite(true);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let Some(path) = entry_path(&entry.path()?) else {
            continue;
        };
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with(".wh."))
        {
            continue;
        }
        let kind = entry.header().entry_type();
        let target = rootfs.join(&path);
        // A file replacing a directory (or the reverse) needs the old one gone.
        if let Ok(meta) = fs::symlink_metadata(&target) {
            if !(meta.is_dir() && kind.is_dir()) {
                remove_any(&target);
            }
        }
        if let Err(e) = entry.unpack_in(rootfs) {
            if kind.is_character_special() || kind.is_block_special() {
                // Device nodes cannot be created in a user namespace; the
                // container gets its own /dev anyway.
                tracing::debug!("skipping device node {:?}: {}", path, e);
                continue;
            }
            return Err(e).with_context(|| format!("unpacking {:?}", path));
        }
    }
    Ok(())
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
            r("docker.io/library/busybox:latest"),
            s("docker.io", "library/busybox", "latest")
        );
        assert_eq!(
            r("bitnami/redis"),
            s("docker.io", "bitnami/redis", "latest")
        );
        assert_eq!(
            r("localhost:5000/app"),
            s("localhost:5000", "app", "latest")
        );
        assert_eq!(
            r("ghcr.io/org/app@sha256:abc"),
            s("ghcr.io", "org/app", "sha256:abc")
        );
        assert_eq!(
            ImageRef::parse("busybox").unwrap().canonical(),
            "docker.io/library/busybox:latest"
        );
    }

    #[test]
    fn parses_bearer_challenge() {
        let p = parse_challenge(
            r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/busybox:pull""#,
        )
        .unwrap();
        assert_eq!(p["realm"], "https://auth.docker.io/token");
        assert_eq!(p["service"], "registry.docker.io");
        assert_eq!(p["scope"], "repository:library/busybox:pull");
        assert!(parse_challenge("Basic realm=\"x\"").is_none());
    }

    #[test]
    fn rejects_escaping_entries() {
        assert_eq!(
            entry_path(Path::new("./etc/passwd")),
            Some(PathBuf::from("etc/passwd"))
        );
        assert_eq!(
            entry_path(Path::new("/bin/sh")),
            Some(PathBuf::from("bin/sh"))
        );
        assert_eq!(entry_path(Path::new("../../etc")), None);
    }

    fn layer(entries: &[(&str, Option<&str>)]) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().unwrap();
        let mut b = tar::Builder::new(f.reopen().unwrap());
        for (path, content) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_uid(0);
            h.set_gid(0);
            h.set_mtime(0);
            match content {
                Some(c) => {
                    h.set_entry_type(tar::EntryType::Regular);
                    h.set_size(c.len() as u64);
                    h.set_mode(0o644);
                    b.append_data(&mut h, path, c.as_bytes()).unwrap();
                }
                None => {
                    h.set_entry_type(tar::EntryType::Directory);
                    h.set_size(0);
                    h.set_mode(0o755);
                    b.append_data(&mut h, path, &[][..]).unwrap();
                }
            }
        }
        b.finish().unwrap();
        f
    }

    #[test]
    fn layers_keep_extended_attributes() {
        // Probe: the test file system must take user xattrs at all.
        let root = tempfile::tempdir().unwrap();
        let probe = root.path().join("probe");
        fs::write(&probe, "").unwrap();
        if xattr::set(&probe, "user.probe", b"1").is_err() {
            eprintln!("skipped: no user xattrs on {:?}", root.path());
            return;
        }
        // A file with an xattr as a PAX record, as image builders write a
        // file capability (`SCHILY.xattr.security.capability`).
        let f = tempfile::NamedTempFile::new().unwrap();
        let mut b = tar::Builder::new(f.reopen().unwrap());
        b.append_pax_extensions([("SCHILY.xattr.user.rusternetes", &b"cap"[..])])
            .unwrap();
        let mut h = tar::Header::new_ustar();
        h.set_uid(0);
        h.set_gid(0);
        h.set_mtime(0);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_size(1);
        h.set_mode(0o755);
        b.append_data(&mut h, "coredns", &b"x"[..]).unwrap();
        b.finish().unwrap();

        apply_layer(f.path(), root.path()).unwrap();
        assert_eq!(
            xattr::get(root.path().join("coredns"), "user.rusternetes").unwrap(),
            Some(b"cap".to_vec())
        );
    }

    #[test]
    fn layers_apply_with_whiteouts() {
        let root = tempfile::tempdir().unwrap();
        let l1 = layer(&[
            ("etc/", None),
            ("etc/a", Some("1")),
            ("etc/b", Some("2")),
            ("opt/", None),
            ("opt/x", Some("x")),
        ]);
        let l2 = layer(&[
            ("etc/.wh.a", Some("")),
            ("opt/.wh..wh..opq", Some("")),
            ("opt/y", Some("y")),
            ("etc/b", Some("3")),
        ]);
        apply_layer(l1.path(), root.path()).unwrap();
        apply_layer(l2.path(), root.path()).unwrap();
        let p = root.path();
        assert!(!p.join("etc/a").exists());
        assert_eq!(fs::read_to_string(p.join("etc/b")).unwrap(), "3");
        assert!(!p.join("opt/x").exists());
        assert_eq!(fs::read_to_string(p.join("opt/y")).unwrap(), "y");
        assert!(!p.join("etc/.wh.a").exists());
    }
}
