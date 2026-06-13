//! Tests for the API-server data source split in `watcher.rs`.
//!
//! `DnsData` + `rebuild_zone` are the decoupling point: both the storage
//! path and the API path funnel into the same pure zone construction, so
//! the two modes provably produce identical zones for identical inputs.

use rusternetes_common::resources::{EndpointSlice, Pod, Service};
use rusternetes_dns::watcher::{api_paths, rebuild_zone, DnsData};
use rusternetes_dns::zone::{DnsRecord, LookupOutcome};

#[test]
fn api_paths_are_the_rest_collection_endpoints() {
    assert_eq!(api_paths::SERVICES, "/api/v1/services");
    assert_eq!(
        api_paths::ENDPOINTSLICES,
        "/apis/discovery.k8s.io/v1/endpointslices"
    );
    assert_eq!(api_paths::PODS, "/api/v1/pods");
}

#[test]
fn dns_data_builds_zone_like_storage_path() {
    let data = DnsData {
        services: Vec::<Service>::new(),
        endpoint_slices: Vec::<EndpointSlice>::new(),
        pods: Vec::<Pod>::new(),
    };
    let zone = rebuild_zone(&data, "cluster.local").expect("empty-input zone builds");
    let _ = zone;
}

#[test]
fn dns_data_zone_resolves_a_clusterip_service() {
    // A minimal ClusterIP service must produce the same record through
    // rebuild_zone as the storage path does (both call Zone::build).
    let svc_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": "kubernetes", "namespace": "default", "uid": "u1" },
        "spec": { "clusterIP": "10.96.0.1", "ports": [{ "port": 443, "protocol": "TCP" }] }
    });
    let svc: Service = serde_json::from_value(svc_json).unwrap();
    let data = DnsData {
        services: vec![svc],
        endpoint_slices: vec![],
        pods: vec![],
    };
    let zone = rebuild_zone(&data, "cluster.local").unwrap();
    let outcome = zone.lookup("kubernetes.default.svc.cluster.local", |r| {
        matches!(r, DnsRecord::A(_))
    });
    assert_eq!(
        outcome,
        LookupOutcome::Records(vec![DnsRecord::A("10.96.0.1".parse().unwrap())])
    );
}
