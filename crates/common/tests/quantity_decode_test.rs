//! Regression tests for `deserialize_quantity_map`.
//!
//! Pin that pod / quota / limit-range bodies emitting a Quantity value
//! as a bare JSON `0` / `1` / `1.5` (instead of the documented quoted
//! string) are accepted by our decoder. K8s upstream Go does the same
//! coercion in `pkg/api/resource/quantity.go::UnmarshalJSON`.
//!
//! Without this tolerance, every pod-update body that K8s' client-go
//! sends with a defaulted/zero Quantity in `containers[*].resources`
//! is rejected at column 942 with
//!   `invalid type: integer 0, expected a string`
//! which in turn blocks five `[Conformance]`-labelled tests.

use rusternetes_common::resources::LimitRange;
use rusternetes_common::resources::PersistentVolumeClaim;
use rusternetes_common::resources::Pod;
use rusternetes_common::resources::ResourceQuota;
use serde_json::json;

#[test]
fn pod_decodes_integer_zero_request_quantity() {
    // Mirrors the column-942 case: a pod body where client-go emitted
    // `requests.cpu` as a bare JSON `0`.
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "requests": { "cpu": 0, "memory": "100Mi" },
                    "limits":   { "cpu": "500m", "memory": "200Mi" }
                }
            }]
        }
    });
    let pod: Pod = serde_json::from_value(body).expect("decode pod");
    let reqs = pod.spec.as_ref().unwrap().containers[0]
        .resources
        .as_ref()
        .unwrap()
        .requests
        .as_ref()
        .unwrap();
    // Integer zero must be coerced to the canonical-string form.
    assert_eq!(reqs.get("cpu").map(String::as_str), Some("0"));
    assert_eq!(reqs.get("memory").map(String::as_str), Some("100Mi"));
}

#[test]
fn pod_decodes_float_quantity() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "limits": { "cpu": 1.5, "memory": "1Gi" }
                }
            }]
        }
    });
    let pod: Pod = serde_json::from_value(body).expect("decode pod with float quantity");
    let limits = pod.spec.as_ref().unwrap().containers[0]
        .resources
        .as_ref()
        .unwrap()
        .limits
        .as_ref()
        .unwrap();
    assert_eq!(limits.get("cpu").map(String::as_str), Some("1.5"));
    assert_eq!(limits.get("memory").map(String::as_str), Some("1Gi"));
}

#[test]
fn pod_decodes_canonical_string_quantity() {
    // The happy path that already worked — protect with a regression pin.
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "requests": { "cpu": "100m", "memory": "100Mi" }
                }
            }]
        }
    });
    let pod: Pod = serde_json::from_value(body).expect("decode pod with string quantity");
    let reqs = pod.spec.as_ref().unwrap().containers[0]
        .resources
        .as_ref()
        .unwrap()
        .requests
        .as_ref()
        .unwrap();
    assert_eq!(reqs.get("cpu").map(String::as_str), Some("100m"));
}

#[test]
fn resourcequota_decodes_integer_hard() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "q", "namespace": "default" },
        "spec": {
            "hard": { "pods": 10, "requests.cpu": 0 }
        },
        "status": {
            "hard": { "pods": 10 },
            "used": { "pods": 0, "requests.cpu": "0" }
        }
    });
    let rq: ResourceQuota = serde_json::from_value(body).expect("decode ResourceQuota");
    let hard = rq.spec.hard.as_ref().unwrap();
    assert_eq!(hard.get("pods").map(String::as_str), Some("10"));
    assert_eq!(hard.get("requests.cpu").map(String::as_str), Some("0"));
    let used = rq.status.as_ref().unwrap().used.as_ref().unwrap();
    assert_eq!(used.get("pods").map(String::as_str), Some("0"));
}

#[test]
fn limitrange_decodes_integer_max_min() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": { "name": "lr", "namespace": "default" },
        "spec": {
            "limits": [{
                "type": "Container",
                "max":     { "cpu": 2, "memory": "1Gi" },
                "min":     { "cpu": 0, "memory": "16Mi" },
                "default": { "cpu": "500m" }
            }]
        }
    });
    let lr: LimitRange = serde_json::from_value(body).expect("decode LimitRange");
    let item = &lr.spec.limits[0];
    assert_eq!(
        item.max.as_ref().unwrap().get("cpu").map(String::as_str),
        Some("2")
    );
    assert_eq!(
        item.min.as_ref().unwrap().get("cpu").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        item.default
            .as_ref()
            .unwrap()
            .get("cpu")
            .map(String::as_str),
        Some("500m")
    );
}

#[test]
fn pvc_status_decodes_integer_capacity() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": "pvc", "namespace": "default" },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": "1Gi" } }
        },
        "status": {
            "phase": "Bound",
            "capacity": { "storage": 0 },
            "allocatedResources": { "storage": 0 }
        }
    });
    let pvc: PersistentVolumeClaim = serde_json::from_value(body).expect("decode PVC");
    let status = pvc.status.as_ref().unwrap();
    assert_eq!(
        status
            .capacity
            .as_ref()
            .unwrap()
            .get("storage")
            .map(String::as_str),
        Some("0")
    );
    assert_eq!(
        status
            .allocated_resources
            .as_ref()
            .unwrap()
            .get("storage")
            .map(String::as_str),
        Some("0")
    );
}

#[test]
fn pod_decodes_omitted_resources_block() {
    // A pod body with no resources field at all must still decode.
    // Defends the `default` + `skip_serializing_if` attributes.
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": "default" },
        "spec": {
            "containers": [{ "name": "c", "image": "pause:latest" }]
        }
    });
    let pod: Pod = serde_json::from_value(body).expect("decode pod without resources");
    assert!(pod.spec.as_ref().unwrap().containers[0].resources.is_none());
}
