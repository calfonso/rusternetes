// Copyright The Rusternetes Authors
// SPDX-License-Identifier: Apache-2.0

//! Conformance: OCI image volume source (`spec.volumes[*].image`).
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e_node/image_volume.go` (the e2e behaviour — node-local,
//!     not in `test/e2e/common/node/`).
//!   - `staging/src/k8s.io/api/core/v1/types.go::ImageVolumeSource` —
//!     `reference` + `pullPolicy` fields are both optional; pullPolicy
//!     follows the same `Always|Never|IfNotPresent` enum as Container.
//!
//! Pins the wire shape so kubelet's mount layer (`runtime.rs::
//! create_pod_volumes`) receives the exact fields a client expects to
//! send via Pod manifests.

use rusternetes_common::resources::{ImageVolumeSource, Pod, Volume};
use serde_json::json;

fn image_volume(name: &str, reference: Option<&str>, policy: Option<&str>) -> Volume {
    Volume {
        name: name.to_string(),
        image: Some(ImageVolumeSource {
            reference: reference.map(str::to_string),
            pull_policy: policy.map(str::to_string),
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
        projected: None,
    }
}

#[test]
fn image_volume_source_serializes_with_camel_case() {
    let v = image_volume(
        "data",
        Some("quay.io/example/data:1.0"),
        Some("IfNotPresent"),
    );
    let json = serde_json::to_value(&v).unwrap();
    assert_eq!(json["name"], "data");
    assert_eq!(json["image"]["reference"], "quay.io/example/data:1.0");
    assert_eq!(json["image"]["pullPolicy"], "IfNotPresent");
}

#[test]
fn pod_decodes_kubectl_style_image_volume_manifest() {
    let body = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "p", "namespace": "default" },
        "spec": {
            "containers": [],
            "volumes": [{
                "name": "data",
                "image": {
                    "reference": "quay.io/example/data:v2",
                    "pullPolicy": "IfNotPresent"
                }
            }]
        }
    });
    let pod: Pod = serde_json::from_value(body).unwrap();
    let v = &pod.spec.unwrap().volumes.unwrap()[0];
    assert_eq!(v.name, "data");
    let img = v.image.as_ref().unwrap();
    assert_eq!(img.reference.as_deref(), Some("quay.io/example/data:v2"));
    assert_eq!(img.pull_policy.as_deref(), Some("IfNotPresent"));
}
