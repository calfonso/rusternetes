//! ServiceAccount admission helpers shared by the api-server (HTTP pod-create
//! admission) and the controller-manager (controllers that create pods by
//! writing directly to storage, bypassing the HTTP admission chain).
//!
//! K8s ref: `plugin/pkg/admission/serviceaccount/admission.go`. The kubelet's
//! projected `kube-api-access-*` volume carries three sources — the bound SA
//! token, the cluster CA (`kube-root-ca.crt`), and the pod namespace — mounted
//! read-only at `/var/run/secrets/kubernetes.io/serviceaccount`.

use crate::resources::{
    ConfigMapProjection, DownwardAPIProjection, DownwardAPIVolumeFile, KeyToPath,
    ObjectFieldSelector, PodSpec, ProjectedVolumeSource, ServiceAccountTokenProjection, Volume,
    VolumeMount, VolumeProjection,
};

/// Canonical name of the injected projected volume.
pub const SA_TOKEN_VOLUME_NAME: &str = "kube-api-access";
/// Canonical mount path of the SA token volume.
pub const SA_TOKEN_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Ensure `spec.service_account_name` is set, defaulting to `"default"`.
/// Returns the effective service account name.
pub fn ensure_service_account_name(spec: &mut PodSpec) -> String {
    match &spec.service_account_name {
        Some(name) => name.clone(),
        None => {
            spec.service_account_name = Some("default".to_string());
            "default".to_string()
        }
    }
}

/// Build the projected `kube-api-access` SA-token volume (token + ca.crt +
/// namespace), matching upstream `TokenVolumeSource()`.
fn sa_token_volume() -> Volume {
    Volume {
        name: SA_TOKEN_VOLUME_NAME.to_string(),
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
            sources: Some(vec![
                VolumeProjection {
                    service_account_token: Some(ServiceAccountTokenProjection {
                        path: "token".to_string(),
                        expiration_seconds: Some(3607),
                        audience: None,
                    }),
                    config_map: None,
                    secret: None,
                    downward_api: None,
                    cluster_trust_bundle: None,
                },
                VolumeProjection {
                    service_account_token: None,
                    config_map: Some(ConfigMapProjection {
                        name: Some("kube-root-ca.crt".to_string()),
                        items: Some(vec![KeyToPath {
                            key: "ca.crt".to_string(),
                            path: "ca.crt".to_string(),
                            mode: None,
                        }]),
                        optional: None,
                    }),
                    secret: None,
                    downward_api: None,
                    cluster_trust_bundle: None,
                },
                VolumeProjection {
                    service_account_token: None,
                    config_map: None,
                    secret: None,
                    downward_api: Some(DownwardAPIProjection {
                        items: Some(vec![DownwardAPIVolumeFile {
                            path: "namespace".to_string(),
                            field_ref: Some(ObjectFieldSelector {
                                api_version: Some("v1".to_string()),
                                field_path: "metadata.namespace".to_string(),
                            }),
                            resource_field_ref: None,
                            mode: None,
                        }]),
                    }),
                    cluster_trust_bundle: None,
                },
            ]),
            default_mode: Some(0o644),
        }),
        image: None,
    }
}

/// Inject the `kube-api-access` projected volume into `spec` and mount it
/// read-only into every container and init container. Idempotent: a volume or
/// mount that is already present (by name / mount path) is left untouched.
///
/// Callers are responsible for the `automountServiceAccountToken` decision —
/// only call this when the token should be mounted.
pub fn add_kube_api_access_volume(spec: &mut PodSpec) {
    match &mut spec.volumes {
        Some(volumes) => {
            if !volumes.iter().any(|v| v.name == SA_TOKEN_VOLUME_NAME) {
                volumes.push(sa_token_volume());
            }
        }
        None => spec.volumes = Some(vec![sa_token_volume()]),
    }

    let mount = VolumeMount {
        name: SA_TOKEN_VOLUME_NAME.to_string(),
        mount_path: SA_TOKEN_MOUNT_PATH.to_string(),
        read_only: Some(true),
        sub_path: None,
        sub_path_expr: None,
        mount_propagation: None,
        recursive_read_only: None,
    };

    let add_mount = |mounts: &mut Option<Vec<VolumeMount>>| match mounts {
        Some(list) => {
            if !list.iter().any(|m| m.mount_path == SA_TOKEN_MOUNT_PATH) {
                list.push(mount.clone());
            }
        }
        None => *mounts = Some(vec![mount.clone()]),
    };

    for container in &mut spec.containers {
        add_mount(&mut container.volume_mounts);
    }
    if let Some(init_containers) = &mut spec.init_containers {
        for container in init_containers {
            add_mount(&mut container.volume_mounts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::Container;

    fn spec_with_container() -> PodSpec {
        PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn ensure_service_account_name_defaults() {
        let mut spec = spec_with_container();
        assert_eq!(ensure_service_account_name(&mut spec), "default");
        assert_eq!(spec.service_account_name.as_deref(), Some("default"));
        spec.service_account_name = Some("custom".to_string());
        assert_eq!(ensure_service_account_name(&mut spec), "custom");
    }

    #[test]
    fn add_volume_injects_three_sources_and_mounts() {
        let mut spec = spec_with_container();
        add_kube_api_access_volume(&mut spec);
        let vol = spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == SA_TOKEN_VOLUME_NAME)
            .expect("volume injected");
        let sources = vol.projected.as_ref().unwrap().sources.as_ref().unwrap();
        assert_eq!(sources.len(), 3, "token + ca.crt + namespace");
        let mount = spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.mount_path == SA_TOKEN_MOUNT_PATH)
            .expect("mount injected");
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn add_volume_is_idempotent() {
        let mut spec = spec_with_container();
        add_kube_api_access_volume(&mut spec);
        add_kube_api_access_volume(&mut spec);
        assert_eq!(
            spec.volumes
                .as_ref()
                .unwrap()
                .iter()
                .filter(|v| v.name == SA_TOKEN_VOLUME_NAME)
                .count(),
            1
        );
        assert_eq!(
            spec.containers[0]
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .filter(|m| m.mount_path == SA_TOKEN_MOUNT_PATH)
                .count(),
            1
        );
    }
}
