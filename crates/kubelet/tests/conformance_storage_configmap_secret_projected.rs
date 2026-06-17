//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-storage] ConfigMap + Secret + Projected volumes.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/storage/
//!   - configmap_volume.go
//!   - secrets_volume.go
//!   - projected_configmap.go
//!   - projected_secret.go
//!   - projected_downwardapi.go
//!   - projected_combined.go
//!
//!
//! See docs/conformance/storage-configmap-secret-projected.md for the
//! test-by-test status table.
//!
//! These tests are pure-function mirrors: they exercise the volume-build
//! semantics that `kubelet/src/runtime.rs::create_volume` implements for
//! ConfigMap, Secret, and Projected sources without standing up a Docker
//! daemon. They replicate the file-writing and permission logic against a
//! `tempfile::TempDir` so each test runs in milliseconds.
//!
//! Coverage taxonomy:
//!   - ConfigMap volume (mode + items + defaultMode + optional + nested
//!     paths + binaryData)
//!   - Secret volume (mode + items + defaultMode + fsGroup + optional +
//!     stringData precedence)
//!   - Projected volume (composing ConfigMap, Secret, DownwardAPI, and
//!     ServiceAccountToken sources into a single mount)
//!
//! R160 had two `fsGroup` + non-root reader failures (secret + projected
//! secret variants); both were fixed by PR #87 (`fix(kubelet): add fsGroup
//! + supplementalGroups to container GIDs`) and the two tests in this
//! file now PASS instead of being `#[ignore]`d. See the "Resolved
//! failures (R160 → fixed)" section in
//! docs/conformance/storage-configmap-secret-projected.md for the root
//! cause (the missing `HostConfig.group_add` wiring, not a chown bug).

use std::collections::{BTreeMap, HashMap, HashSet};

use rusternetes_common::resources::{
    ConfigMap, ConfigMapProjection, ConfigMapVolumeSource, DownwardAPIProjection,
    DownwardAPIVolumeFile, KeyToPath, ObjectFieldSelector, Pod, PodSpec, ProjectedVolumeSource,
    Secret, SecretProjection, SecretVolumeSource, ServiceAccountTokenProjection, Volume,
    VolumeProjection,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};

// ---------------------------------------------------------------------------
// Helpers — Pod / ConfigMap / Secret constructors
// ---------------------------------------------------------------------------

fn make_pod_with_volumes(name: &str, namespace: &str, volumes: Vec<Volume>) -> Pod {
    let mut spec = PodSpec {
        containers: Vec::new(),
        volumes: Some(volumes),
        ..PodSpec::default()
    };
    spec.restart_policy = Some("Always".to_string());
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(spec),
        status: None,
    }
}

fn cm_with_data(name: &str, namespace: &str, data: &[(&str, &str)]) -> ConfigMap {
    let mut map = HashMap::new();
    for (k, v) in data {
        map.insert(k.to_string(), v.to_string());
    }
    let mut cm = ConfigMap::new(name, namespace);
    cm.data = Some(map);
    cm
}

fn cm_with_binary(name: &str, namespace: &str, entries: &[(&str, &[u8])]) -> ConfigMap {
    let mut map = HashMap::new();
    for (k, v) in entries {
        map.insert(k.to_string(), v.to_vec());
    }
    let mut cm = ConfigMap::new(name, namespace);
    cm.binary_data = Some(map);
    cm
}

fn secret_with_data(name: &str, namespace: &str, data: &[(&str, &[u8])]) -> Secret {
    let mut map = HashMap::new();
    for (k, v) in data {
        map.insert(k.to_string(), v.to_vec());
    }
    let mut s = Secret::new(name, namespace);
    s.data = Some(map);
    s
}

// ---------------------------------------------------------------------------
// Helpers — pure volume-build replicas of `runtime.rs::create_volume`
// ---------------------------------------------------------------------------

/// Replica of the ConfigMap branch of `create_volume`. Writes ConfigMap
/// entries to the given directory honouring `items` / `defaultMode` /
/// `optional`. Returns the list of files actually written (relative).
fn build_configmap_volume(
    volume_dir: &std::path::Path,
    source: &ConfigMapVolumeSource,
    cm: Option<&ConfigMap>,
) -> std::io::Result<Vec<String>> {
    std::fs::create_dir_all(volume_dir)?;
    let default_mode = source.default_mode.unwrap_or(0o644);
    let mut written = Vec::new();
    let Some(cm) = cm else {
        if source.optional.unwrap_or(false) {
            return Ok(written);
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "ConfigMap {:?} not found and not optional",
                source.name.as_deref().unwrap_or("<unnamed>")
            ),
        ));
    };
    if let Some(items) = &source.items {
        for item in items {
            let mode = item.mode.unwrap_or(default_mode);
            let file_path = volume_dir.join(&item.path);
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if let Some(value) = cm.data.as_ref().and_then(|d| d.get(&item.key)) {
                std::fs::write(&file_path, value.as_bytes())?;
                set_mode(&file_path, mode);
                written.push(item.path.clone());
            } else if let Some(value) = cm.binary_data.as_ref().and_then(|d| d.get(&item.key)) {
                std::fs::write(&file_path, value)?;
                set_mode(&file_path, mode);
                written.push(item.path.clone());
            }
        }
        return Ok(written);
    }
    if let Some(data) = &cm.data {
        for (k, v) in data {
            let file_path = volume_dir.join(k);
            std::fs::write(&file_path, v.as_bytes())?;
            set_mode(&file_path, default_mode);
            written.push(k.clone());
        }
    }
    if let Some(bdata) = &cm.binary_data {
        for (k, v) in bdata {
            let file_path = volume_dir.join(k);
            std::fs::write(&file_path, v)?;
            set_mode(&file_path, default_mode);
            written.push(k.clone());
        }
    }
    Ok(written)
}

/// Replica of the Secret branch of `create_volume`. Writes Secret entries
/// honouring `items` / `defaultMode` / `optional`. `string_data` (if
/// present) takes precedence over `data` for the same key, matching the
/// upstream API-server merge semantics.
fn build_secret_volume(
    volume_dir: &std::path::Path,
    source: &SecretVolumeSource,
    secret: Option<&Secret>,
) -> std::io::Result<Vec<String>> {
    std::fs::create_dir_all(volume_dir)?;
    let default_mode = source.default_mode.unwrap_or(0o644);
    let mut written = Vec::new();
    let Some(secret) = secret else {
        if source.optional.unwrap_or(false) {
            return Ok(written);
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Secret {:?} not found and not optional",
                source.secret_name.as_deref().unwrap_or("<unnamed>")
            ),
        ));
    };
    // Merge: stringData wins on conflict.
    let mut merged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    if let Some(data) = &secret.data {
        for (k, v) in data {
            merged.insert(k.clone(), v.clone());
        }
    }
    if let Some(sd) = &secret.string_data {
        for (k, v) in sd {
            merged.insert(k.clone(), v.as_bytes().to_vec());
        }
    }
    if let Some(items) = &source.items {
        for item in items {
            if let Some(value) = merged.get(&item.key) {
                let mode = item.mode.unwrap_or(default_mode);
                let file_path = volume_dir.join(&item.path);
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&file_path, value)?;
                set_mode(&file_path, mode);
                written.push(item.path.clone());
            }
        }
    } else {
        for (k, v) in &merged {
            let file_path = volume_dir.join(k);
            std::fs::write(&file_path, v)?;
            set_mode(&file_path, default_mode);
            written.push(k.clone());
        }
    }
    Ok(written)
}

/// Replica of the Projected branch of `create_volume`. Composes any mix of
/// ConfigMap, Secret, DownwardAPI, and ServiceAccountToken projections into
/// a single directory. Returns the set of relative file paths written.
fn build_projected_volume(
    volume_dir: &std::path::Path,
    source: &ProjectedVolumeSource,
    pod: &Pod,
    configmaps: &HashMap<(String, String), ConfigMap>,
    secrets: &HashMap<(String, String), Secret>,
    sa_token_value: &str,
) -> std::io::Result<HashSet<String>> {
    std::fs::create_dir_all(volume_dir)?;
    let default_mode = source.default_mode.unwrap_or(0o644);
    let namespace = pod
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let mut written = HashSet::new();

    let Some(sources) = &source.sources else {
        set_mode(volume_dir, default_mode | 0o111);
        return Ok(written);
    };

    for proj in sources {
        // ConfigMap projection
        if let Some(cm_proj) = &proj.config_map {
            if let Some(name) = &cm_proj.name {
                let key = (namespace.clone(), name.clone());
                match configmaps.get(&key) {
                    Some(cm) => {
                        if let Some(items) = &cm_proj.items {
                            for item in items {
                                let mode = item.mode.unwrap_or(default_mode);
                                let file_path = volume_dir.join(&item.path);
                                if let Some(parent) = file_path.parent() {
                                    std::fs::create_dir_all(parent)?;
                                }
                                if let Some(v) = cm.data.as_ref().and_then(|d| d.get(&item.key)) {
                                    std::fs::write(&file_path, v.as_bytes())?;
                                    set_mode(&file_path, mode);
                                    written.insert(item.path.clone());
                                } else if let Some(v) =
                                    cm.binary_data.as_ref().and_then(|d| d.get(&item.key))
                                {
                                    std::fs::write(&file_path, v)?;
                                    set_mode(&file_path, mode);
                                    written.insert(item.path.clone());
                                }
                            }
                        } else if let Some(data) = &cm.data {
                            for (k, v) in data {
                                let file_path = volume_dir.join(k);
                                std::fs::write(&file_path, v.as_bytes())?;
                                set_mode(&file_path, default_mode);
                                written.insert(k.clone());
                            }
                        }
                    }
                    None if cm_proj.optional.unwrap_or(false) => {
                        // skip silently — matches runtime warn-and-skip
                    }
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("projected ConfigMap {name} not found"),
                        ));
                    }
                }
            }
        }
        // Secret projection
        if let Some(secret_proj) = &proj.secret {
            if let Some(name) = &secret_proj.name {
                let key = (namespace.clone(), name.clone());
                match secrets.get(&key) {
                    Some(secret) => {
                        if let Some(data) = &secret.data {
                            if let Some(items) = &secret_proj.items {
                                for item in items {
                                    if let Some(value) = data.get(&item.key) {
                                        let mode = item.mode.unwrap_or(default_mode);
                                        let file_path = volume_dir.join(&item.path);
                                        if let Some(parent) = file_path.parent() {
                                            std::fs::create_dir_all(parent)?;
                                        }
                                        std::fs::write(&file_path, value)?;
                                        set_mode(&file_path, mode);
                                        written.insert(item.path.clone());
                                    }
                                }
                            } else {
                                for (k, v) in data {
                                    let file_path = volume_dir.join(k);
                                    std::fs::write(&file_path, v)?;
                                    set_mode(&file_path, default_mode);
                                    written.insert(k.clone());
                                }
                            }
                        }
                    }
                    None if secret_proj.optional.unwrap_or(false) => {}
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("projected Secret {name} not found"),
                        ));
                    }
                }
            }
        }
        // DownwardAPI projection
        if let Some(dapi) = &proj.downward_api {
            if let Some(items) = &dapi.items {
                for item in items {
                    let file_path = volume_dir.join(&item.path);
                    if let Some(parent) = file_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let value = item
                        .field_ref
                        .as_ref()
                        .map(|fr| resolve_pod_field(pod, &fr.field_path))
                        .unwrap_or_default();
                    std::fs::write(&file_path, &value)?;
                    let mode = item.mode.unwrap_or(default_mode);
                    set_mode(&file_path, mode);
                    written.insert(item.path.clone());
                }
            }
        }
        // ServiceAccountToken projection
        if let Some(sat) = &proj.service_account_token {
            let file_path = volume_dir.join(&sat.path);
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&file_path, sa_token_value.as_bytes())?;
            set_mode(&file_path, default_mode);
            written.insert(sat.path.clone());
        }
    }

    // Directory mode applied last so restrictive defaults don't block writes.
    set_mode(volume_dir, default_mode | 0o111);
    Ok(written)
}

#[cfg(unix)]
fn set_mode(path: &std::path::Path, mode: i32) {
    use std::os::unix::fs::PermissionsExt;
    if path.exists() {
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode as u32));
    }
}

#[cfg(not(unix))]
fn set_mode(_path: &std::path::Path, _mode: i32) {}

#[cfg(unix)]
fn mode_of(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

/// Resolve a DownwardAPI `field_ref.field_path` against a Pod, mirroring
/// `runtime.rs::get_pod_field_value`.
fn resolve_pod_field(pod: &Pod, field_path: &str) -> String {
    match field_path {
        "metadata.name" => pod.metadata.name.clone(),
        "metadata.namespace" => pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        "metadata.uid" => pod.metadata.uid.clone(),
        _ => String::new(),
    }
}

// ===========================================================================
// ConfigMap volume tests
// ===========================================================================

/// [sig-storage] ConfigMap should be consumable from pods in volume [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:48
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn configmap_volume_should_be_consumable_from_pods() {
    let tmp = tempfile::tempdir().unwrap();
    let cm = cm_with_data(
        "cm-1",
        "ns",
        &[("data-1", "value-1"), ("data-2", "value-2")],
    );
    let source = ConfigMapVolumeSource {
        name: Some("cm-1".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    let written = build_configmap_volume(tmp.path(), &source, Some(&cm)).unwrap();
    assert_eq!(written.len(), 2);
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("data-1")).unwrap(),
        "value-1"
    );
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("data-2")).unwrap(),
        "value-2"
    );
}

/// [sig-storage] ConfigMap should be consumable from pods in volume with defaultMode set [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:62
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[cfg(unix)]
#[test]
fn configmap_volume_should_be_consumable_with_defaultmode_set() {
    let tmp = tempfile::tempdir().unwrap();
    let cm = cm_with_data("cm-mode", "ns", &[("data-1", "value-1")]);
    let source = ConfigMapVolumeSource {
        name: Some("cm-mode".to_string()),
        items: None,
        default_mode: Some(0o400),
        optional: None,
    };
    build_configmap_volume(tmp.path(), &source, Some(&cm)).unwrap();
    assert_eq!(mode_of(&tmp.path().join("data-1")), 0o400);
}

/// [sig-storage] ConfigMap should be consumable from pods in volume as non-root [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:84
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn configmap_volume_should_be_consumable_as_non_root() {
    // For the pure-function mirror the non-root signal is "file mode is
    // group/world-readable", since we cannot fork uid in tests.
    let tmp = tempfile::tempdir().unwrap();
    let cm = cm_with_data("cm-nr", "ns", &[("data-1", "value-1")]);
    let source = ConfigMapVolumeSource {
        name: Some("cm-nr".to_string()),
        items: None,
        default_mode: Some(0o644),
        optional: None,
    };
    build_configmap_volume(tmp.path(), &source, Some(&cm)).unwrap();
    #[cfg(unix)]
    assert_eq!(mode_of(&tmp.path().join("data-1")) & 0o044, 0o044);
}

/// [sig-storage] ConfigMap should be consumable from pods in volume with mappings [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:108
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn configmap_volume_should_be_consumable_with_mappings() {
    let tmp = tempfile::tempdir().unwrap();
    let cm = cm_with_data(
        "cm-map",
        "ns",
        &[("data-1", "value-1"), ("data-2", "value-2")],
    );
    let source = ConfigMapVolumeSource {
        name: Some("cm-map".to_string()),
        items: Some(vec![KeyToPath {
            key: "data-2".to_string(),
            path: "path/to/data-2".to_string(),
            mode: None,
        }]),
        default_mode: None,
        optional: None,
    };
    let written = build_configmap_volume(tmp.path(), &source, Some(&cm)).unwrap();
    assert_eq!(written, vec!["path/to/data-2".to_string()]);
    assert!(!tmp.path().join("data-1").exists());
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("path/to/data-2")).unwrap(),
        "value-2"
    );
}

/// [sig-storage] ConfigMap should be consumable from pods in volume with mappings and Item Mode set [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:134
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[cfg(unix)]
#[test]
fn configmap_volume_should_be_consumable_with_mappings_and_item_mode_set() {
    let tmp = tempfile::tempdir().unwrap();
    let cm = cm_with_data("cm-itm", "ns", &[("data-2", "value-2")]);
    let source = ConfigMapVolumeSource {
        name: Some("cm-itm".to_string()),
        items: Some(vec![KeyToPath {
            key: "data-2".to_string(),
            path: "path/to/data-2".to_string(),
            mode: Some(0o400),
        }]),
        default_mode: Some(0o644),
        optional: None,
    };
    build_configmap_volume(tmp.path(), &source, Some(&cm)).unwrap();
    assert_eq!(mode_of(&tmp.path().join("path/to/data-2")), 0o400);
}

/// [sig-storage] ConfigMap optional updates should be reflected in volume [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:233
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn configmap_volume_optional_missing_should_create_empty_volume() {
    let tmp = tempfile::tempdir().unwrap();
    let source = ConfigMapVolumeSource {
        name: Some("missing-cm".to_string()),
        items: None,
        default_mode: None,
        optional: Some(true),
    };
    let written = build_configmap_volume(tmp.path(), &source, None).unwrap();
    assert!(written.is_empty(), "optional missing CM yields empty mount");
    assert!(tmp.path().exists());
}

/// [sig-storage] ConfigMap required missing should fail pod start [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:255
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn configmap_volume_required_missing_should_error() {
    let tmp = tempfile::tempdir().unwrap();
    let source = ConfigMapVolumeSource {
        name: Some("missing-cm".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    let err = build_configmap_volume(tmp.path(), &source, None).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

/// [sig-storage] ConfigMap binaryData should be consumable as files [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/configmap_volume.go:175
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn configmap_volume_binary_data_should_be_consumable() {
    let tmp = tempfile::tempdir().unwrap();
    let cm = cm_with_binary("cm-bin", "ns", &[("blob", &[0x00, 0x01, 0xFE, 0xFF])]);
    let source = ConfigMapVolumeSource {
        name: Some("cm-bin".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    build_configmap_volume(tmp.path(), &source, Some(&cm)).unwrap();
    let bytes = std::fs::read(tmp.path().join("blob")).unwrap();
    assert_eq!(bytes, vec![0x00, 0x01, 0xFE, 0xFF]);
}

// ===========================================================================
// Secret volume tests
// ===========================================================================

/// [sig-storage] Secrets should be consumable from pods in volume [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:47
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn secret_volume_should_be_consumable_from_pods() {
    let tmp = tempfile::tempdir().unwrap();
    let secret = secret_with_data(
        "s-1",
        "ns",
        &[("data-1", b"value-1"), ("data-2", b"value-2")],
    );
    let source = SecretVolumeSource {
        secret_name: Some("s-1".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    let written = build_secret_volume(tmp.path(), &source, Some(&secret)).unwrap();
    assert_eq!(written.len(), 2);
    assert_eq!(
        std::fs::read(tmp.path().join("data-1")).unwrap(),
        b"value-1"
    );
}

/// [sig-storage] Secrets should be consumable from pods in volume with defaultMode set [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:61
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[cfg(unix)]
#[test]
fn secret_volume_should_be_consumable_with_defaultmode_set() {
    let tmp = tempfile::tempdir().unwrap();
    let secret = secret_with_data("s-mode", "ns", &[("data-1", b"value-1")]);
    let source = SecretVolumeSource {
        secret_name: Some("s-mode".to_string()),
        items: None,
        default_mode: Some(0o400),
        optional: None,
    };
    build_secret_volume(tmp.path(), &source, Some(&secret)).unwrap();
    assert_eq!(mode_of(&tmp.path().join("data-1")), 0o400);
}

/// [sig-storage] Secrets should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:73
/// Sonobuoy (Round 160): was FAIL; fixed by `fix(kubelet): add fsGroup +
/// supplementalGroups to container GIDs` (PR #87). The two layers that
/// have to line up are now both covered here: the volume layer writes the
/// requested mode, and the container-arg layer surfaces `fsGroup` as a
/// supplementary GID via `runtime::compute_group_add`, so a non-root
/// `runAsUser` ends up in `fsGroup` and reads the mode-0o440 file.
#[cfg(unix)]
#[test]
fn secret_volume_should_be_consumable_as_non_root_with_defaultmode_and_fsgroup() {
    let tmp = tempfile::tempdir().unwrap();
    let secret = secret_with_data("s-nr", "ns", &[("data-1", b"value-1")]);
    let source = SecretVolumeSource {
        secret_name: Some("s-nr".to_string()),
        items: None,
        default_mode: Some(0o440),
        optional: None,
    };
    build_secret_volume(tmp.path(), &source, Some(&secret)).unwrap();

    // Volume layer: file is written with the requested mode (0o440).
    // `apply_fs_group_to_volumes` later chowns it to `:fsGroup` and copies
    // owner bits to group bits — that's an in-process effect on real
    // pods and not exercised by this pure-function test.
    assert_eq!(mode_of(&tmp.path().join("data-1")), 0o440);
    assert_eq!(
        std::fs::read(tmp.path().join("data-1")).unwrap(),
        b"value-1"
    );

    // Container-arg layer: a pod with `securityContext.fsGroup` must
    // surface that GID via `compute_group_add` so the container is
    // launched with `--group-add <fsGroup>` and the non-root runAsUser
    // gains the group membership needed to read mode 0o440.
    let mut pod = make_pod_with_volumes("s-nr-pod", "ns", vec![]);
    pod.spec.as_mut().unwrap().security_context =
        Some(rusternetes_common::resources::pod::PodSecurityContext {
            fs_group: Some(2000),
            ..Default::default()
        });
    assert_eq!(
        rusternetes_kubelet::runtime::compute_group_add(&pod),
        Some(vec!["2000".to_string()]),
        "fsGroup must flow into HostConfig.group_add so non-root runAsUser \
         joins the group that owns mode-0o440 secret files"
    );
}

/// [sig-storage] Secrets should be consumable from pods in volume with mappings [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:106
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn secret_volume_should_be_consumable_with_mappings() {
    let tmp = tempfile::tempdir().unwrap();
    let secret = secret_with_data(
        "s-map",
        "ns",
        &[("data-1", b"value-1"), ("data-2", b"value-2")],
    );
    let source = SecretVolumeSource {
        secret_name: Some("s-map".to_string()),
        items: Some(vec![KeyToPath {
            key: "data-1".to_string(),
            path: "new-path-data-1".to_string(),
            mode: None,
        }]),
        default_mode: None,
        optional: None,
    };
    let written = build_secret_volume(tmp.path(), &source, Some(&secret)).unwrap();
    assert_eq!(written, vec!["new-path-data-1".to_string()]);
    assert!(!tmp.path().join("data-1").exists());
    assert_eq!(
        std::fs::read(tmp.path().join("new-path-data-1")).unwrap(),
        b"value-1"
    );
}

/// [sig-storage] Secrets should be consumable from pods in volume with mappings and Item Mode set [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:133
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[cfg(unix)]
#[test]
fn secret_volume_should_be_consumable_with_mappings_and_item_mode_set() {
    let tmp = tempfile::tempdir().unwrap();
    let secret = secret_with_data("s-itm", "ns", &[("data-1", b"value-1")]);
    let source = SecretVolumeSource {
        secret_name: Some("s-itm".to_string()),
        items: Some(vec![KeyToPath {
            key: "data-1".to_string(),
            path: "new-path-data-1".to_string(),
            mode: Some(0o400),
        }]),
        default_mode: Some(0o644),
        optional: None,
    };
    build_secret_volume(tmp.path(), &source, Some(&secret)).unwrap();
    assert_eq!(mode_of(&tmp.path().join("new-path-data-1")), 0o400);
}

/// [sig-storage] Secrets optional should not fail pod start [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:300
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn secret_volume_optional_missing_should_create_empty_volume() {
    let tmp = tempfile::tempdir().unwrap();
    let source = SecretVolumeSource {
        secret_name: Some("missing-secret".to_string()),
        items: None,
        default_mode: None,
        optional: Some(true),
    };
    let written = build_secret_volume(tmp.path(), &source, None).unwrap();
    assert!(written.is_empty());
    assert!(tmp.path().exists());
}

/// [sig-storage] Secrets required missing should fail pod start [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:320
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn secret_volume_required_missing_should_error() {
    let tmp = tempfile::tempdir().unwrap();
    let source = SecretVolumeSource {
        secret_name: Some("missing-secret".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    let err = build_secret_volume(tmp.path(), &source, None).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

/// [sig-storage] Secrets stringData should override data when both are set
///
/// Upstream: covered by ApiMachinery secret-validation; mirrors the kubelet
/// merge order used for volume materialisation.
/// Sonobuoy (Round 160, 2026-04-26): PASS (implicit via secret consumption tests)
#[test]
fn secret_volume_string_data_overrides_data_on_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let mut secret = secret_with_data("s-mix", "ns", &[("k", b"binary-wins?")]);
    let mut sd = HashMap::new();
    sd.insert("k".to_string(), "string-wins".to_string());
    secret.string_data = Some(sd);
    let source = SecretVolumeSource {
        secret_name: Some("s-mix".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    build_secret_volume(tmp.path(), &source, Some(&secret)).unwrap();
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("k")).unwrap(),
        "string-wins"
    );
}

// ===========================================================================
// Projected volume tests
// ===========================================================================

/// [sig-storage] Projected configMap should be consumable from pods in volume [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_configmap.go:44
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_configmap_should_be_consumable_from_pods() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let cm = cm_with_data("p-cm", "ns", &[("data-1", "value-1")]);
    let mut cms = HashMap::new();
    cms.insert(("ns".to_string(), "p-cm".to_string()), cm);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: Some(ConfigMapProjection {
                name: Some("p-cm".to_string()),
                items: None,
                optional: None,
            }),
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let written =
        build_projected_volume(tmp.path(), &source, &pod, &cms, &HashMap::new(), "").unwrap();
    assert!(written.contains("data-1"));
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("data-1")).unwrap(),
        "value-1"
    );
}

/// [sig-storage] Projected configMap should be consumable in volume with mappings [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_configmap.go:120
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_configmap_should_be_consumable_with_mappings() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let cm = cm_with_data("p-cm", "ns", &[("data-1", "v1"), ("data-2", "v2")]);
    let mut cms = HashMap::new();
    cms.insert(("ns".to_string(), "p-cm".to_string()), cm);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: Some(ConfigMapProjection {
                name: Some("p-cm".to_string()),
                items: Some(vec![KeyToPath {
                    key: "data-2".to_string(),
                    path: "path/data-2".to_string(),
                    mode: None,
                }]),
                optional: None,
            }),
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let written =
        build_projected_volume(tmp.path(), &source, &pod, &cms, &HashMap::new(), "").unwrap();
    assert!(written.contains("path/data-2"));
    assert!(!tmp.path().join("data-1").exists());
}

/// [sig-storage] Projected secret should be consumable from pods in volume [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_secret.go:44
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_secret_should_be_consumable_from_pods() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let secret = secret_with_data("p-s", "ns", &[("data-1", b"value-1")]);
    let mut ss = HashMap::new();
    ss.insert(("ns".to_string(), "p-s".to_string()), secret);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: Some(SecretProjection {
                name: Some("p-s".to_string()),
                items: None,
                optional: None,
            }),
            config_map: None,
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let written =
        build_projected_volume(tmp.path(), &source, &pod, &HashMap::new(), &ss, "").unwrap();
    assert!(written.contains("data-1"));
    assert_eq!(
        std::fs::read(tmp.path().join("data-1")).unwrap(),
        b"value-1"
    );
}

/// [sig-storage] Projected secret should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_secret.go:67
/// Sonobuoy (Round 160): was FAIL with file mode `-r--r-----` and
/// `permission denied`; fixed by `fix(kubelet): add fsGroup +
/// supplementalGroups to container GIDs` (PR #87). Same two-layer assertion
/// as the secret-volume test above: volume layer writes the mode, container
/// layer surfaces fsGroup as a supplementary GID so the non-root reader
/// inherits group membership for the chowned file.
#[cfg(unix)]
#[test]
fn projected_secret_should_be_consumable_as_non_root_with_defaultmode_and_fsgroup() {
    let tmp = tempfile::tempdir().unwrap();
    let mut pod = make_pod_with_volumes("p", "ns", vec![]);
    pod.spec.as_mut().unwrap().security_context =
        Some(rusternetes_common::resources::pod::PodSecurityContext {
            fs_group: Some(2000),
            ..Default::default()
        });
    let secret = secret_with_data("p-s-nr", "ns", &[("data-1", b"value-1")]);
    let mut ss = HashMap::new();
    ss.insert(("ns".to_string(), "p-s-nr".to_string()), secret);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: Some(SecretProjection {
                name: Some("p-s-nr".to_string()),
                items: None,
                optional: None,
            }),
            config_map: None,
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: Some(0o440),
    };
    build_projected_volume(tmp.path(), &source, &pod, &HashMap::new(), &ss, "").unwrap();

    // Volume layer: file written with the requested mode (0o440).
    assert_eq!(mode_of(&tmp.path().join("data-1")), 0o440);
    assert_eq!(
        std::fs::read(tmp.path().join("data-1")).unwrap(),
        b"value-1"
    );

    // Container-arg layer: fsGroup must surface as a supplementary GID
    // so the non-root runAsUser inherits the group that owns the file
    // after `apply_fs_group_to_volumes` chowns it.
    assert_eq!(
        rusternetes_kubelet::runtime::compute_group_add(&pod),
        Some(vec!["2000".to_string()]),
        "projected-secret + fsGroup must wire HostConfig.group_add"
    );
}

/// [sig-storage] Projected secret should be consumable in volume with mappings [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_secret.go:106
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_secret_should_be_consumable_with_mappings() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let secret = secret_with_data("p-s-map", "ns", &[("data-1", b"v1")]);
    let mut ss = HashMap::new();
    ss.insert(("ns".to_string(), "p-s-map".to_string()), secret);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: Some(SecretProjection {
                name: Some("p-s-map".to_string()),
                items: Some(vec![KeyToPath {
                    key: "data-1".to_string(),
                    path: "new-path".to_string(),
                    mode: None,
                }]),
                optional: None,
            }),
            config_map: None,
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let written =
        build_projected_volume(tmp.path(), &source, &pod, &HashMap::new(), &ss, "").unwrap();
    assert!(written.contains("new-path"));
    assert!(!tmp.path().join("data-1").exists());
}

/// [sig-storage] Projected downwardAPI should provide podname only [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_downwardapi.go:48
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_downwardapi_should_provide_podname_only() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("podname-x", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: None,
            service_account_token: None,
            downward_api: Some(DownwardAPIProjection {
                items: Some(vec![DownwardAPIVolumeFile {
                    path: "podname".to_string(),
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.name".to_string(),
                        api_version: None,
                    }),
                    resource_field_ref: None,
                    mode: None,
                }]),
            }),
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "",
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("podname")).unwrap(),
        "podname-x"
    );
}

/// [sig-storage] Projected downwardAPI should set DefaultMode on files [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_downwardapi.go:80
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[cfg(unix)]
#[test]
fn projected_downwardapi_should_set_defaultmode_on_files() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("any", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: None,
            service_account_token: None,
            downward_api: Some(DownwardAPIProjection {
                items: Some(vec![DownwardAPIVolumeFile {
                    path: "podname".to_string(),
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.name".to_string(),
                        api_version: None,
                    }),
                    resource_field_ref: None,
                    mode: None,
                }]),
            }),
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: Some(0o400),
    };
    build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "",
    )
    .unwrap();
    assert_eq!(mode_of(&tmp.path().join("podname")), 0o400);
}

/// [sig-storage] Projected downwardAPI should set mode on item file [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_downwardapi.go:107
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[cfg(unix)]
#[test]
fn projected_downwardapi_should_set_mode_on_item_file() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("any", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: None,
            service_account_token: None,
            downward_api: Some(DownwardAPIProjection {
                items: Some(vec![DownwardAPIVolumeFile {
                    path: "podname".to_string(),
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.name".to_string(),
                        api_version: None,
                    }),
                    resource_field_ref: None,
                    mode: Some(0o400),
                }]),
            }),
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: Some(0o644),
    };
    build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "",
    )
    .unwrap();
    assert_eq!(mode_of(&tmp.path().join("podname")), 0o400);
}

/// [sig-storage] Projected combined should project all components into the same directory [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_combined.go:44
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_combined_should_project_all_components_into_same_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("combo", "ns", vec![]);
    let cm = cm_with_data("combo-cm", "ns", &[("from-cm", "cmval")]);
    let secret = secret_with_data("combo-s", "ns", &[("from-s", b"sval")]);
    let mut cms = HashMap::new();
    cms.insert(("ns".to_string(), "combo-cm".to_string()), cm);
    let mut ss = HashMap::new();
    ss.insert(("ns".to_string(), "combo-s".to_string()), secret);

    let source = ProjectedVolumeSource {
        sources: Some(vec![
            VolumeProjection {
                secret: None,
                config_map: Some(ConfigMapProjection {
                    name: Some("combo-cm".to_string()),
                    items: None,
                    optional: None,
                }),
                service_account_token: None,
                downward_api: None,
                cluster_trust_bundle: None,
                ..Default::default()
            },
            VolumeProjection {
                secret: Some(SecretProjection {
                    name: Some("combo-s".to_string()),
                    items: None,
                    optional: None,
                }),
                config_map: None,
                service_account_token: None,
                downward_api: None,
                cluster_trust_bundle: None,
                ..Default::default()
            },
            VolumeProjection {
                secret: None,
                config_map: None,
                service_account_token: None,
                downward_api: Some(DownwardAPIProjection {
                    items: Some(vec![DownwardAPIVolumeFile {
                        path: "ns".to_string(),
                        field_ref: Some(ObjectFieldSelector {
                            field_path: "metadata.namespace".to_string(),
                            api_version: None,
                        }),
                        resource_field_ref: None,
                        mode: None,
                    }]),
                }),
                cluster_trust_bundle: None,
                ..Default::default()
            },
        ]),
        default_mode: None,
    };
    let written = build_projected_volume(tmp.path(), &source, &pod, &cms, &ss, "").unwrap();
    assert!(written.contains("from-cm"));
    assert!(written.contains("from-s"));
    assert!(written.contains("ns"));
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("ns")).unwrap(),
        "ns"
    );
}

/// [sig-storage] Projected serviceAccountToken should mount projected SA token [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go (projected
/// SAT path), mirrored at projected-volume-source ServiceAccountTokenProjection.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_serviceaccount_token_should_be_mounted_at_path() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("sat", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: None,
            service_account_token: Some(ServiceAccountTokenProjection {
                path: "token".to_string(),
                audience: Some("aud-x".to_string()),
                expiration_seconds: Some(3600),
            }),
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "jwt.placeholder.token",
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("token")).unwrap(),
        "jwt.placeholder.token"
    );
}

/// [sig-storage] Projected secret optional updates should not fail pod start [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_secret.go:228
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_secret_optional_missing_should_skip_source() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: Some(SecretProjection {
                name: Some("does-not-exist".to_string()),
                items: None,
                optional: Some(true),
            }),
            config_map: None,
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let written = build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "",
    )
    .unwrap();
    assert!(written.is_empty());
}

/// [sig-storage] Projected configmap optional updates should not fail pod start [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/projected_configmap.go:236
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_configmap_optional_missing_should_skip_source() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: Some(ConfigMapProjection {
                name: Some("does-not-exist".to_string()),
                items: None,
                optional: Some(true),
            }),
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let written = build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "",
    )
    .unwrap();
    assert!(written.is_empty());
}

/// [sig-storage] Projected required missing configmap should error [NodeConformance] [Conformance]
///
/// Upstream: derived from projected_configmap.go behavioural contract.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn projected_required_missing_configmap_should_error() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod_with_volumes("p", "ns", vec![]);
    let source = ProjectedVolumeSource {
        sources: Some(vec![VolumeProjection {
            secret: None,
            config_map: Some(ConfigMapProjection {
                name: Some("missing-required".to_string()),
                items: None,
                optional: None,
            }),
            service_account_token: None,
            downward_api: None,
            cluster_trust_bundle: None,
            ..Default::default()
        }]),
        default_mode: None,
    };
    let err = build_projected_volume(
        tmp.path(),
        &source,
        &pod,
        &HashMap::new(),
        &HashMap::new(),
        "",
    )
    .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

// ===========================================================================
// Volume-source wiring smoke tests
// ===========================================================================

/// Smoke test: a Volume that wraps a ConfigMap can be round-tripped through
/// the Pod resource without losing the source. Guards against camelCase
/// rename regressions on the wire.
#[test]
fn pod_volume_with_configmap_source_round_trips_through_serde() {
    let pod = make_pod_with_volumes(
        "p",
        "ns",
        vec![Volume {
            name: "cm-vol".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some("my-cm".to_string()),
                items: None,
                default_mode: Some(0o644),
                optional: None,
            }),
            empty_dir: None,
            host_path: None,
            secret: None,
            persistent_volume_claim: None,
            downward_api: None,
            csi: None,
            ephemeral: None,
            nfs: None,
            iscsi: None,
            projected: None,
            image: None,
        }],
    );
    let json = serde_json::to_string(&pod).unwrap();
    assert!(json.contains("\"configMap\""));
    assert!(json.contains("\"defaultMode\":420")); // 0o644 = 420
    let parsed: Pod = serde_json::from_str(&json).unwrap();
    let v = &parsed.spec.unwrap().volumes.unwrap()[0];
    assert_eq!(v.config_map.as_ref().unwrap().default_mode, Some(0o644));
}

/// Smoke test: a Volume wrapping a Secret round-trips with the historical
/// `secretName` field name preserved.
#[test]
fn pod_volume_with_secret_source_uses_secret_name_field() {
    let pod = make_pod_with_volumes(
        "p",
        "ns",
        vec![Volume {
            name: "s-vol".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some("my-s".to_string()),
                items: None,
                default_mode: Some(0o400),
                optional: None,
            }),
            empty_dir: None,
            host_path: None,
            config_map: None,
            persistent_volume_claim: None,
            downward_api: None,
            csi: None,
            ephemeral: None,
            nfs: None,
            iscsi: None,
            projected: None,
            image: None,
        }],
    );
    let json = serde_json::to_string(&pod).unwrap();
    assert!(json.contains("\"secretName\":\"my-s\""));
    assert!(json.contains("\"defaultMode\":256")); // 0o400 = 256
}

/// Smoke test: a Volume wrapping a Projected source preserves the nested
/// `sources` list across serde.
#[test]
fn pod_volume_with_projected_source_preserves_sources_list() {
    let pod = make_pod_with_volumes(
        "p",
        "ns",
        vec![Volume {
            name: "proj-vol".to_string(),
            projected: Some(ProjectedVolumeSource {
                sources: Some(vec![
                    VolumeProjection {
                        secret: Some(SecretProjection {
                            name: Some("s1".to_string()),
                            items: None,
                            optional: None,
                        }),
                        config_map: None,
                        service_account_token: None,
                        downward_api: None,
                        cluster_trust_bundle: None,
                        ..Default::default()
                    },
                    VolumeProjection {
                        secret: None,
                        config_map: Some(ConfigMapProjection {
                            name: Some("c1".to_string()),
                            items: None,
                            optional: None,
                        }),
                        service_account_token: None,
                        downward_api: None,
                        cluster_trust_bundle: None,
                        ..Default::default()
                    },
                ]),
                default_mode: Some(0o644),
            }),
            empty_dir: None,
            host_path: None,
            config_map: None,
            secret: None,
            persistent_volume_claim: None,
            downward_api: None,
            csi: None,
            ephemeral: None,
            nfs: None,
            iscsi: None,
            image: None,
        }],
    );
    let json = serde_json::to_string(&pod).unwrap();
    assert!(json.contains("\"projected\""));
    let parsed: Pod = serde_json::from_str(&json).unwrap();
    let v = &parsed.spec.unwrap().volumes.unwrap()[0];
    let proj = v.projected.as_ref().unwrap();
    assert_eq!(proj.sources.as_ref().unwrap().len(), 2);
}
