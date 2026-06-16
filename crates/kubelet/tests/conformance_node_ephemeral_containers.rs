// Copyright The Rusternetes Authors
// SPDX-License-Identifier: Apache-2.0

//! Conformance: EphemeralContainer resource shape.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/ephemeral_containers.go` — the e2e behaviour.
//!   - `staging/src/k8s.io/api/core/v1/types.go::EphemeralContainer` —
//!     the wire shape, including which fields are inherited from `Container`
//!     and which are intentionally absent (`ports`, `livenessProbe`,
//!     `readinessProbe`, `startupProbe`, `lifecycle`, `resources` ...).
//!
//! These tests pin the resource shape: serde round-tripping, omission of
//! optional fields, and the `targetContainerName` "process namespace
//! sharing" path. They complement `ephemeral_containers_test.rs` (which
//! covers in-pod mutation of `spec.ephemeralContainers`).

use rusternetes_common::resources::{EphemeralContainer, Pod};
use serde_json::json;

fn ec(name: &str, target: Option<&str>) -> EphemeralContainer {
    EphemeralContainer {
        name: name.to_string(),
        image: "busybox:latest".to_string(),
        command: Some(vec!["sh".to_string()]),
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: Some("IfNotPresent".to_string()),
        security_context: None,
        target_container_name: target.map(str::to_string),
        stdin: Some(true),
        stdin_once: Some(false),
        tty: Some(true),
        resize_policy: None,
        restart_policy: None,
        resources: None,
        termination_message_path: None,
        termination_message_policy: None,
        ..Default::default()
    }
}

#[test]
fn ephemeral_container_serializes_with_camel_case_fields() {
    let e = ec("debugger", Some("app"));
    let v = serde_json::to_value(&e).unwrap();

    // Mandatory.
    assert_eq!(v["name"], "debugger");
    assert_eq!(v["image"], "busybox:latest");

    // K8s wire format uses camelCase — verify the bridge fields land in the
    // exact form a client-go decoder expects.
    assert_eq!(v["targetContainerName"], "app");
    assert_eq!(v["imagePullPolicy"], "IfNotPresent");
    assert_eq!(v["stdin"], true);
    assert_eq!(v["stdinOnce"], false);
    assert_eq!(v["tty"], true);
}

#[test]
fn ephemeral_container_does_not_advertise_probe_fields() {
    // Upstream EphemeralContainer intentionally drops probes & ports.
    // Pin that by asserting the JSON does not contain those keys even
    // through serde defaults (they aren't fields on the struct).
    let e = ec("debugger", None);
    let v = serde_json::to_value(&e).unwrap();
    for forbidden in [
        "ports",
        "livenessProbe",
        "readinessProbe",
        "startupProbe",
        "lifecycle",
    ] {
        assert!(
            v.get(forbidden).is_none(),
            "EphemeralContainer must not expose `{forbidden}`"
        );
    }
}

#[test]
fn ephemeral_container_decodes_kubectl_debug_style_request_body() {
    // Mirrors the JSON body `kubectl debug` sends to the
    // `/api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers` subresource:
    // a Pod with only `spec.ephemeralContainers` populated.
    let body = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "p", "namespace": "default" },
        "spec": {
            "containers": [],
            "ephemeralContainers": [{
                "name": "debugger",
                "image": "busybox:1.36",
                "targetContainerName": "main",
                "stdin": true,
                "tty": true
            }]
        }
    });
    let pod: Pod = serde_json::from_value(body).unwrap();
    let ecs = pod.spec.unwrap().ephemeral_containers.unwrap();
    assert_eq!(ecs.len(), 1);
    assert_eq!(ecs[0].name, "debugger");
    assert_eq!(ecs[0].target_container_name.as_deref(), Some("main"));
}
