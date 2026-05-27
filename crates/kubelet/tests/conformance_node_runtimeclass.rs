// Copyright The Rusternetes Authors
// SPDX-License-Identifier: Apache-2.0

//! Conformance: `RuntimeClass` resource shape + `Pod.spec.runtimeClassName`.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/runtimeclass.go`
//!   - `staging/src/k8s.io/api/node/v1/types.go`
//!
//! Rusternetes' kubelet uses a single hardcoded Docker runtime — runtime
//! *selection* is not implemented. These tests pin the resource API
//! surface only: the kubelet must still accept and round-trip
//! `RuntimeClass` objects and Pod manifests that reference
//! `runtimeClassName`, so client tooling (`kubectl get runtimeclasses`)
//! doesn't break. Behaviour (actually running the named runtime) is
//! deliberately out of scope.

use rusternetes_common::resources::runtimeclass::{Overhead, RuntimeClass};
use rusternetes_common::resources::Pod;
use std::collections::HashMap;

#[test]
fn runtime_class_uses_node_k8s_io_v1_api_version() {
    let rc = RuntimeClass::new("runc", "runc");
    assert_eq!(rc.type_meta.kind, "RuntimeClass");
    assert_eq!(rc.type_meta.api_version, "node.k8s.io/v1");
}

#[test]
fn runtime_class_handler_is_required_field_in_json() {
    let rc = RuntimeClass::new("kata", "kata-runtime");
    let v = serde_json::to_value(&rc).unwrap();
    assert_eq!(v["handler"], "kata-runtime");
    assert_eq!(v["metadata"]["name"], "kata");
}

#[test]
fn runtime_class_overhead_pod_fixed_uses_camel_case() {
    let mut pf = HashMap::new();
    pf.insert("cpu".to_string(), "250m".to_string());
    pf.insert("memory".to_string(), "120Mi".to_string());
    let rc = RuntimeClass::new("kata", "kata-runtime").with_overhead(Overhead {
        pod_fixed: Some(pf),
    });
    let v = serde_json::to_value(&rc).unwrap();
    assert_eq!(v["overhead"]["podFixed"]["cpu"], "250m");
    assert_eq!(v["overhead"]["podFixed"]["memory"], "120Mi");
}

#[test]
fn pod_runtime_class_name_round_trips_through_spec() {
    let body = serde_json::json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "p", "namespace": "default" },
        "spec": {
            "containers": [],
            "runtimeClassName": "gvisor"
        }
    });
    let pod: Pod = serde_json::from_value(body).unwrap();
    assert_eq!(
        pod.spec.unwrap().runtime_class_name.as_deref(),
        Some("gvisor")
    );
}
