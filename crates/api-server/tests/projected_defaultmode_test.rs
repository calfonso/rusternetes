//! Integration test for projected/secret volume `defaultMode` bits.
//!
//! Covers the K8s API server defaulting path that backs the upstream e2e
//! `framework/pod/output/output.go:282` failure ("should be consumable from
//! pods in volume as non-root with defaultMode and fsGroup set"). The pod
//! lands on the kubelet with `defaultMode = None`, the kubelet falls back
//! to its own hardcoded 0o644, and the file ends up with the wrong bits.
//!
//! The fix lives in `handlers::secret::apply_volume_mode_defaults`, which
//! mirrors `SetDefaults_SecretVolumeSource`, `SetDefaults_ConfigMapVolumeSource`,
//! `SetDefaults_DownwardAPIVolumeSource`, and `SetDefaults_ProjectedVolumeSource`
//! from `pkg/apis/core/v1/defaults.go`.

use rusternetes_api_server::handlers::secret::apply_volume_mode_defaults;
use rusternetes_common::resources::pod::{
    ConfigMapVolumeSource, DownwardAPIVolumeSource, KeyToPath, ProjectedVolumeSource,
    SecretProjection, SecretVolumeSource, Volume, VolumeProjection,
};
use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

fn empty_container() -> Container {
    Container {
        name: "c".to_string(),
        image: "busybox:latest".to_string(),
        ..Default::default()
    }
}

fn pod_with_volumes(name: &str, volumes: Vec<Volume>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![empty_container()],
            volumes: Some(volumes),
            ..Default::default()
        }),
        status: None,
    }
}

/// Caller-specified `defaultMode` on a projected Secret must round-trip
/// unchanged through the api-server defaulting path AND through storage.
#[tokio::test]
async fn projected_secret_explicit_default_mode_round_trips() {
    let projected_volume = Volume {
        name: "projected-secret-volume".to_string(),
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
        projected: Some(ProjectedVolumeSource {
            // 0o644 (decimal 420) — matches what the e2e test sends.
            default_mode: Some(0o644),
            sources: Some(vec![VolumeProjection {
                secret: Some(SecretProjection {
                    name: Some("my-secret".to_string()),
                    items: Some(vec![KeyToPath {
                        key: "data-1".to_string(),
                        path: "data-1".to_string(),
                        // 0o600 — owner-only. Per-item mode wins over defaultMode.
                        mode: Some(0o600),
                    }]),
                    optional: None,
                }),
                config_map: None,
                service_account_token: None,
                downward_api: None,
                cluster_trust_bundle: None,
                ..Default::default()
            }]),
        }),
        image: None,
    };

    let mut pod = pod_with_volumes("p-explicit", vec![projected_volume]);

    apply_volume_mode_defaults(pod.spec.as_mut().unwrap());

    // Explicit defaultMode must survive defaulting unchanged.
    let v = &pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0];
    let proj = v.projected.as_ref().unwrap();
    assert_eq!(
        proj.default_mode,
        Some(0o644),
        "explicit projected defaultMode must round-trip"
    );
    let item = &proj.sources.as_ref().unwrap()[0]
        .secret
        .as_ref()
        .unwrap()
        .items
        .as_ref()
        .unwrap()[0];
    assert_eq!(
        item.mode,
        Some(0o600),
        "per-item mode must round-trip and not be clobbered by defaultMode"
    );

    // Now round-trip the Pod through storage and re-check.
    let storage = Arc::new(MemoryStorage::new());
    let key = build_key("pods", Some("default"), "p-explicit");
    storage.create(&key, &pod).await.unwrap();
    let fetched: Pod = storage.get(&key).await.unwrap();

    let fv = &fetched.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0];
    let fproj = fv.projected.as_ref().unwrap();
    assert_eq!(fproj.default_mode, Some(0o644));
    let fitem = &fproj.sources.as_ref().unwrap()[0]
        .secret
        .as_ref()
        .unwrap()
        .items
        .as_ref()
        .unwrap()[0];
    assert_eq!(fitem.mode, Some(0o600));
}

/// Omitted `defaultMode` on a projected volume must default to 0o644 (decimal 420).
#[tokio::test]
async fn projected_omitted_default_mode_defaults_to_0644() {
    let projected_volume = Volume {
        name: "p-vol".to_string(),
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
        projected: Some(ProjectedVolumeSource {
            default_mode: None,
            sources: Some(vec![VolumeProjection {
                secret: Some(SecretProjection {
                    name: Some("my-secret".to_string()),
                    items: None,
                    optional: None,
                }),
                config_map: None,
                service_account_token: None,
                downward_api: None,
                cluster_trust_bundle: None,
                ..Default::default()
            }]),
        }),
        image: None,
    };

    let mut pod = pod_with_volumes("p-omitted", vec![projected_volume]);
    apply_volume_mode_defaults(pod.spec.as_mut().unwrap());

    let proj = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0]
        .projected
        .as_ref()
        .unwrap();
    assert_eq!(
        proj.default_mode,
        Some(0o644),
        "omitted projected defaultMode must default to 0o644 (decimal 420)"
    );
}

/// Omitted `defaultMode` on a plain Secret volume must default to 0o644.
#[tokio::test]
async fn secret_volume_omitted_default_mode_defaults_to_0644() {
    let secret_volume = Volume {
        name: "sv".to_string(),
        empty_dir: None,
        host_path: None,
        config_map: None,
        secret: Some(SecretVolumeSource {
            secret_name: Some("my-secret".to_string()),
            items: None,
            default_mode: None,
            optional: None,
        }),
        persistent_volume_claim: None,
        downward_api: None,
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    };

    let mut pod = pod_with_volumes("p-sv", vec![secret_volume]);
    apply_volume_mode_defaults(pod.spec.as_mut().unwrap());

    let sv = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0]
        .secret
        .as_ref()
        .unwrap();
    assert_eq!(sv.default_mode, Some(0o644));
}

/// Omitted `defaultMode` on ConfigMap/DownwardAPI volumes must default to 0o644.
#[tokio::test]
async fn configmap_and_downward_api_volumes_default_mode_defaults_to_0644() {
    let cm_volume = Volume {
        name: "cm".to_string(),
        empty_dir: None,
        host_path: None,
        config_map: Some(ConfigMapVolumeSource {
            name: Some("my-cm".to_string()),
            items: None,
            default_mode: None,
            optional: None,
        }),
        secret: None,
        persistent_volume_claim: None,
        downward_api: None,
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    };
    let dapi_volume = Volume {
        name: "dapi".to_string(),
        empty_dir: None,
        host_path: None,
        config_map: None,
        secret: None,
        persistent_volume_claim: None,
        downward_api: Some(DownwardAPIVolumeSource {
            items: None,
            default_mode: None,
        }),
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    };

    let mut pod = pod_with_volumes("p-mix", vec![cm_volume, dapi_volume]);
    apply_volume_mode_defaults(pod.spec.as_mut().unwrap());

    let vols = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap();
    assert_eq!(
        vols[0].config_map.as_ref().unwrap().default_mode,
        Some(0o644)
    );
    assert_eq!(
        vols[1].downward_api.as_ref().unwrap().default_mode,
        Some(0o644)
    );
}
