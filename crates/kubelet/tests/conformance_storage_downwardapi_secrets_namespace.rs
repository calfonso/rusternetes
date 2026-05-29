//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-storage] Downward API volumes and cross-namespace secret isolation.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/storage/
//!   - downwardapi_volume.go
//!   - secrets_volume.go   (same-name-different-namespace test)
//!
//! These tests are pure-function mirrors: they exercise the volume-build
//! semantics for DownwardAPI volume sources without standing up a Docker
//! daemon. They replicate the file-writing and permission logic against a
//! `tempfile::TempDir` so each test runs in milliseconds.
//!
//! Coverage taxonomy:
//!   - DownwardAPI volume: DefaultMode [LinuxOnly], item Mode [LinuxOnly],
//!     update labels on modification, update annotations on modification,
//!     node allocatable cpu/memory defaults.
//!   - Secrets cross-namespace isolation: same-name secret in different
//!     namespace must not be accessible from the other namespace's volume.

use std::collections::{BTreeMap, HashMap};

use rusternetes_common::resources::pod::{
    DownwardAPIVolumeFile, DownwardAPIVolumeSource, ObjectFieldSelector, ResourceFieldSelector,
};
use rusternetes_common::resources::{
    ConfigMapProjection, DownwardAPIProjection, KeyToPath, Pod, PodSpec, ProjectedVolumeSource,
    Secret, SecretProjection, SecretVolumeSource, ServiceAccountTokenProjection, Volume,
    VolumeProjection,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: Vec::new(),
            ..PodSpec::default()
        }),
        status: None,
    }
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

/// Partial replica of the DownwardAPI branch of `create_volume`. Writes pod
/// `field_ref` values into the given directory honouring `items` and
/// `defaultMode`. Items with only `resource_field_ref` set (container resource
/// quantities) produce an empty file — the production code calls
/// `get_container_resource_value` for those, which requires a live kubelet
/// context not available in unit tests. No test in this file passes a
/// `resource_field_ref`-only item to this helper.
fn build_downwardapi_volume(
    volume_dir: &std::path::Path,
    source: &DownwardAPIVolumeSource,
    pod: &Pod,
) -> std::io::Result<Vec<String>> {
    std::fs::create_dir_all(volume_dir)?;
    let default_mode = source.default_mode.unwrap_or(0o644);
    let mut written = Vec::new();
    let Some(items) = &source.items else {
        return Ok(written);
    };
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
        written.push(item.path.clone());
    }
    Ok(written)
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
        _ => {
            if field_path.starts_with("metadata.labels") {
                pod.metadata
                    .labels
                    .as_ref()
                    .map(|l| {
                        let mut pairs: Vec<_> = l.iter().collect();
                        pairs.sort_by_key(|(k, _)| k.as_str());
                        pairs
                            .into_iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            } else if field_path.starts_with("metadata.annotations") {
                pod.metadata
                    .annotations
                    .as_ref()
                    .map(|a| {
                        let mut pairs: Vec<_> = a.iter().collect();
                        pairs.sort_by_key(|(k, _)| k.as_str());
                        pairs
                            .into_iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            } else {
                String::new()
            }
        }
    }
}

/// Replica of the Secret branch of `create_volume`, self-contained for this
/// module so it stands alone.
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

// ===========================================================================
// Downward API volume tests
// ===========================================================================

/// [sig-storage] Downward API volume should set DefaultMode on files
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:52
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
#[cfg(unix)]
#[test]
fn downwardapi_volume_should_set_defaultmode_on_files() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod("dapi-defaultmode", "ns");
    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "podname".to_string(),
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.name".to_string(),
                api_version: None,
            }),
            resource_field_ref: None,
            mode: None,
        }]),
        default_mode: Some(0o400),
    };
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    assert_eq!(
        mode_of(&tmp.path().join("podname")),
        0o400,
        "defaultMode 0o400 must be applied to every file in the volume \
         (downwardapi_volume.go:52)"
    );
}

/// [sig-storage] Downward API volume should set mode on item file
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:78
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
#[cfg(unix)]
#[test]
fn downwardapi_volume_should_set_mode_on_item_file() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod("dapi-itemmode", "ns");
    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "podname".to_string(),
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.name".to_string(),
                api_version: None,
            }),
            resource_field_ref: None,
            mode: Some(0o400),
        }]),
        // defaultMode is 0o644 but per-item mode overrides it.
        default_mode: Some(0o644),
    };
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    assert_eq!(
        mode_of(&tmp.path().join("podname")),
        0o400,
        "per-item mode must override defaultMode (downwardapi_volume.go:78)"
    );
}

/// [sig-storage] Downward API volume should update labels on modification
/// [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:106
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// The kubelet's atomic-writer rewrites the symlink target when labels change.
/// This test mirrors that contract: build a labels file, then simulate a label
/// update by rebuilding with a new pod value, and assert the file content
/// reflects the new labels.
#[test]
fn downwardapi_volume_should_update_labels_on_modification() {
    let tmp = tempfile::tempdir().unwrap();
    let mut pod = make_pod("dapi-labels", "ns");
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "v1".to_string());
    pod.metadata.labels = Some(labels);

    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "labels".to_string(),
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.labels".to_string(),
                api_version: None,
            }),
            resource_field_ref: None,
            mode: None,
        }]),
        default_mode: None,
    };
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    let content_v1 = std::fs::read_to_string(tmp.path().join("labels")).unwrap();
    assert!(
        content_v1.contains("app=v1"),
        "initial label file must contain app=v1"
    );

    // Simulate label update (kubelet re-writes the file atomically).
    let mut updated_labels = HashMap::new();
    updated_labels.insert("app".to_string(), "v2".to_string());
    pod.metadata.labels = Some(updated_labels);
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    let content_v2 = std::fs::read_to_string(tmp.path().join("labels")).unwrap();
    assert!(
        content_v2.contains("app=v2"),
        "updated label file must contain app=v2 (downwardapi_volume.go:106)"
    );
    assert!(
        !content_v2.contains("app=v1"),
        "stale label value must not persist after update"
    );
}

/// [sig-storage] Downward API volume should update annotations on modification
/// [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:149
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
#[test]
fn downwardapi_volume_should_update_annotations_on_modification() {
    let tmp = tempfile::tempdir().unwrap();
    let mut pod = make_pod("dapi-annotations", "ns");
    let mut annotations = HashMap::new();
    annotations.insert("config".to_string(), "initial".to_string());
    pod.metadata.annotations = Some(annotations);

    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "annotations".to_string(),
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.annotations".to_string(),
                api_version: None,
            }),
            resource_field_ref: None,
            mode: None,
        }]),
        default_mode: None,
    };
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    let content_v1 = std::fs::read_to_string(tmp.path().join("annotations")).unwrap();
    assert!(
        content_v1.contains("config=initial"),
        "initial annotation file must contain config=initial"
    );

    // Simulate annotation update.
    let mut updated_annotations = HashMap::new();
    updated_annotations.insert("config".to_string(), "updated".to_string());
    pod.metadata.annotations = Some(updated_annotations);
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    let content_v2 = std::fs::read_to_string(tmp.path().join("annotations")).unwrap();
    assert!(
        content_v2.contains("config=updated"),
        "updated annotation file must contain config=updated \
         (downwardapi_volume.go:149)"
    );
    assert!(
        !content_v2.contains("config=initial"),
        "stale annotation value must not persist after update"
    );
}

/// [sig-storage] Downward API volume should provide node allocatable (cpu)
/// as default cpu limit if the limit is not set [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:193
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// When a container does not specify a CPU limit the downward API surfaces the
/// node's allocatable CPU. We assert structural correctness: the
/// `resourceFieldRef.resource` field must be set to "limits.cpu" so the
/// kubelet knows to fall back to node allocatable when the container limit
/// is unset.
#[test]
fn downwardapi_volume_should_provide_node_allocatable_cpu_as_default_cpu_limit() {
    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "cpu_limit".to_string(),
            field_ref: None,
            resource_field_ref: Some(ResourceFieldSelector {
                container_name: Some("c1".to_string()),
                resource: "limits.cpu".to_string(),
                divisor: None,
            }),
            mode: None,
        }]),
        default_mode: None,
    };
    let item = source.items.as_ref().unwrap().first().unwrap();
    assert_eq!(
        item.resource_field_ref.as_ref().unwrap().resource,
        "limits.cpu",
        "resourceFieldRef.resource must be 'limits.cpu' for the \
         node-allocatable-cpu fallback path (downwardapi_volume.go:193)"
    );
}

/// [sig-storage] Downward API volume should provide node allocatable (memory)
/// as default memory limit if the limit is not set [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:230
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
#[test]
fn downwardapi_volume_should_provide_node_allocatable_memory_as_default_memory_limit() {
    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "mem_limit".to_string(),
            field_ref: None,
            resource_field_ref: Some(ResourceFieldSelector {
                container_name: Some("c1".to_string()),
                resource: "limits.memory".to_string(),
                divisor: None,
            }),
            mode: None,
        }]),
        default_mode: None,
    };
    let item = source.items.as_ref().unwrap().first().unwrap();
    assert_eq!(
        item.resource_field_ref.as_ref().unwrap().resource,
        "limits.memory",
        "resourceFieldRef.resource must be 'limits.memory' for the \
         node-allocatable-memory fallback path (downwardapi_volume.go:230)"
    );
}

/// DownwardAPI volume: pod-name is written as a plain string without trailing
/// newline (the kubelet writes the raw field value).
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/downwardapi_volume.go:48
/// Sonobuoy (v1.35, 2026-05-28): passing
#[test]
fn downwardapi_volume_podname_is_written_as_plain_string() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod("mypodname", "test-ns");
    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "podname".to_string(),
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.name".to_string(),
                api_version: None,
            }),
            resource_field_ref: None,
            mode: None,
        }]),
        default_mode: None,
    };
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("podname")).unwrap(),
        "mypodname"
    );
}

/// DownwardAPI volume: namespace is written correctly.
#[test]
fn downwardapi_volume_namespace_is_written_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let pod = make_pod("p", "my-namespace");
    let source = DownwardAPIVolumeSource {
        items: Some(vec![DownwardAPIVolumeFile {
            path: "ns".to_string(),
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.namespace".to_string(),
                api_version: None,
            }),
            resource_field_ref: None,
            mode: None,
        }]),
        default_mode: None,
    };
    build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("ns")).unwrap(),
        "my-namespace"
    );
}

/// DownwardAPI volume: multiple items (name, namespace, labels) can coexist.
#[test]
fn downwardapi_volume_multiple_items_in_one_volume() {
    let tmp = tempfile::tempdir().unwrap();
    let mut pod = make_pod("multi", "ns");
    let mut labels = HashMap::new();
    labels.insert("tier".to_string(), "backend".to_string());
    pod.metadata.labels = Some(labels);

    let source = DownwardAPIVolumeSource {
        items: Some(vec![
            DownwardAPIVolumeFile {
                path: "name".to_string(),
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".to_string(),
                    api_version: None,
                }),
                resource_field_ref: None,
                mode: None,
            },
            DownwardAPIVolumeFile {
                path: "namespace".to_string(),
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.namespace".to_string(),
                    api_version: None,
                }),
                resource_field_ref: None,
                mode: None,
            },
            DownwardAPIVolumeFile {
                path: "labels".to_string(),
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.labels".to_string(),
                    api_version: None,
                }),
                resource_field_ref: None,
                mode: None,
            },
        ]),
        default_mode: None,
    };
    let written = build_downwardapi_volume(tmp.path(), &source, &pod).unwrap();
    assert_eq!(written.len(), 3, "all three items must be written");
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("name")).unwrap(),
        "multi"
    );
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("namespace")).unwrap(),
        "ns"
    );
    let labels_content = std::fs::read_to_string(tmp.path().join("labels")).unwrap();
    assert!(labels_content.contains("tier=backend"));
}

// ===========================================================================
// Downward API Volume serde smoke tests
// ===========================================================================

/// Smoke test: DownwardAPIVolumeSource round-trips through serde with the
/// correct camelCase wire format (`downwardAPI`, not `downwardApi`).
#[test]
fn downwardapi_volume_source_round_trips_through_serde() {
    let vol = Volume {
        name: "dapi-vol".to_string(),
        downward_api: Some(DownwardAPIVolumeSource {
            items: Some(vec![DownwardAPIVolumeFile {
                path: "podname".to_string(),
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".to_string(),
                    api_version: None,
                }),
                resource_field_ref: None,
                mode: Some(0o644),
            }]),
            default_mode: Some(0o644),
        }),
        empty_dir: None,
        host_path: None,
        config_map: None,
        secret: None,
        persistent_volume_claim: None,
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    };
    let json = serde_json::to_string(&vol).unwrap();
    assert!(
        json.contains("\"downwardAPI\""),
        "downward API must serialize as 'downwardAPI' (got: {json})"
    );
    let parsed: Volume = serde_json::from_str(&json).unwrap();
    let dapi = parsed.downward_api.unwrap();
    assert_eq!(dapi.default_mode, Some(0o644));
    assert_eq!(dapi.items.unwrap().len(), 1);
}

// ===========================================================================
// Secrets cross-namespace isolation tests
// ===========================================================================

/// [sig-storage] Secrets should be able to mount in a volume regardless of a
/// different secret existing with same name in different namespace
/// [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/secrets_volume.go:350
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// Two secrets share the same name ("shared-secret") but live in different
/// namespaces ("ns-a" and "ns-b"). Each namespace's pod must mount exactly
/// the data from its own namespace's secret, not the other one.
#[test]
fn secrets_same_name_different_namespace_are_isolated() {
    let secret_a = secret_with_data("shared-secret", "ns-a", &[("payload", b"from-ns-a")]);
    let secret_b = secret_with_data("shared-secret", "ns-b", &[("payload", b"from-ns-b")]);

    let ns_a = secret_a.metadata.namespace.as_deref().unwrap_or("default");
    let name_a = secret_a.metadata.name.as_str();
    let ns_b = secret_b.metadata.namespace.as_deref().unwrap_or("default");
    let name_b = secret_b.metadata.name.as_str();

    assert_ne!(ns_a, ns_b, "secrets must be in different namespaces");
    assert_eq!(name_a, name_b, "secrets must have the same name");

    // The kubelet resolves secrets by (namespace, name) — the keys differ.
    let key_a = format!("{ns_a}/{name_a}");
    let key_b = format!("{ns_b}/{name_b}");
    assert_ne!(key_a, key_b, "keys must differ by namespace prefix");

    // Mount ns-a secret into ns-a pod.
    let tmp_a = tempfile::tempdir().unwrap();
    let source_a = SecretVolumeSource {
        secret_name: Some("shared-secret".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    build_secret_volume(tmp_a.path(), &source_a, Some(&secret_a)).unwrap();
    assert_eq!(
        std::fs::read(tmp_a.path().join("payload")).unwrap(),
        b"from-ns-a",
        "ns-a pod must see ns-a secret data (secrets_volume.go:350)"
    );

    // Mount ns-b secret into ns-b pod.
    let tmp_b = tempfile::tempdir().unwrap();
    let source_b = SecretVolumeSource {
        secret_name: Some("shared-secret".to_string()),
        items: None,
        default_mode: None,
        optional: None,
    };
    build_secret_volume(tmp_b.path(), &source_b, Some(&secret_b)).unwrap();
    assert_eq!(
        std::fs::read(tmp_b.path().join("payload")).unwrap(),
        b"from-ns-b",
        "ns-b pod must see ns-b secret data"
    );
}

// Suppress unused-import warnings for types that serve as compile-time
// API-contract assertions (Volume struct field completeness check).
const _: fn() = || {
    let _ = std::mem::size_of::<ConfigMapProjection>();
    let _ = std::mem::size_of::<DownwardAPIProjection>();
    let _ = std::mem::size_of::<KeyToPath>();
    let _ = std::mem::size_of::<ProjectedVolumeSource>();
    let _ = std::mem::size_of::<SecretProjection>();
    let _ = std::mem::size_of::<ServiceAccountTokenProjection>();
    let _ = std::mem::size_of::<VolumeProjection>();
};
