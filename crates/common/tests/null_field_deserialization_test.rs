// Regression tests for null-field deserialization fixes.
//
// Real Kubernetes clients (kubectl, go-client) sometimes send JSON with explicit
// `null` for optional fields. The five commits below all fixed deserialization
// panics when these fields were `null`. These tests ensure the fixes stay in
// place by sending exactly the null JSON that triggered each original bug.
//
// Commits covered:
//   2332cf4  fix: ObjectMeta — tolerate null name during deserialization
//   b5e457c  fix: EndpointAddress ip — handle null values like ObjectMeta.name
//   c2364a3  Fix #241: TokenRequest audiences field accepts null
//   ff43065  Fix #212: EndpointSlice ports field accepts null
//   402d503  Fix CSINode null drivers + ResourceQuota status route

use rusternetes_common::resources::{
    CSINodeSpec, EndpointAddress, EndpointSlice, TokenRequestSpec,
};
use rusternetes_common::types::ObjectMeta;
use serde_json::json;

// ---------------------------------------------------------------------------
// 2332cf4 — ObjectMeta.name tolerates null
// ---------------------------------------------------------------------------

#[test]
fn test_object_meta_tolerates_null_name() {
    // kubectl strategic-merge-patch can produce {"name": null} when the patch
    // body omits name. Without deserialize_null_string this returns Err.
    let v = json!({"name": null, "namespace": "default"});
    let m: ObjectMeta = serde_json::from_value(v)
        .expect("ObjectMeta with name=null must deserialize without error");
    // null maps to empty string (Go zero-value behaviour)
    assert_eq!(m.name, "");
}

// ---------------------------------------------------------------------------
// b5e457c — EndpointAddress.ip tolerates null
// ---------------------------------------------------------------------------

#[test]
fn test_endpoint_address_tolerates_null_ip() {
    // Endpoints stored in etcd may have ip: null from protobuf decode or
    // initial creation. Without deserialize_null_string this returns Err.
    let v = json!({"ip": null});
    let a: EndpointAddress = serde_json::from_value(v)
        .expect("EndpointAddress with ip=null must deserialize without error");
    assert_eq!(a.ip, "");
}

// ---------------------------------------------------------------------------
// c2364a3 — TokenRequestSpec.audiences tolerates null
// ---------------------------------------------------------------------------

#[test]
fn test_token_request_spec_tolerates_null_audiences() {
    // The K8s client sends {"audiences": null} when no audiences are specified.
    // Without deserialize_null_default this returns Err ("invalid type: null,
    // expected a sequence").
    let v = json!({"audiences": null});
    let s: TokenRequestSpec = serde_json::from_value(v)
        .expect("TokenRequestSpec with audiences=null must deserialize without error");
    // null → empty Vec (Go zero-value behaviour)
    assert!(s.audiences.is_empty());
}

// ---------------------------------------------------------------------------
// ff43065 — EndpointSlice.ports tolerates null
// ---------------------------------------------------------------------------

#[test]
fn test_endpoint_slice_tolerates_null_ports() {
    // The K8s conformance test sends {"ports": null}. Before the fix,
    // serde(default) handled *missing* fields but not explicit null.
    let v = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "test"},
        "addressType": "IPv4",
        "ports": null
    });
    let es: EndpointSlice = serde_json::from_value(v)
        .expect("EndpointSlice with ports=null must deserialize without error");
    assert!(es.ports.is_empty());
}

// ---------------------------------------------------------------------------
// 402d503 — CSINodeSpec.drivers tolerates null
// ---------------------------------------------------------------------------

#[test]
fn test_csi_node_spec_tolerates_null_drivers() {
    // CSINode kubelet registration can produce {"drivers": null} on first
    // write. Without deserialize_null_default this returns Err.
    let v = json!({"drivers": null});
    let s: CSINodeSpec = serde_json::from_value(v)
        .expect("CSINodeSpec with drivers=null must deserialize without error");
    assert!(s.drivers.is_empty());
}
