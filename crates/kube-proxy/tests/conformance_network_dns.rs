//! Conformance: Service / Pod DNS-relevant wire shape.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/network/dns.go` (e2e behaviour against CoreDNS)
//!   - `test/e2e/network/dns_common.go`
//!   - `staging/src/k8s.io/api/core/v1/types.go::ServiceSpec` for
//!     `clusterIP` / `clusterIPs` / `publishNotReadyAddresses`
//!
//! In Rusternetes the DNS plane is CoreDNS itself; kube-proxy programs
//! the iptables that send `clusterIP` traffic to backing pods. The
//! resource fields below are what CoreDNS, kube-proxy, and the kubelet
//! all read to produce in-cluster names. Pin the wire shape so a future
//! refactor cannot silently break DNS records.
//!
//! No runtime / iptables side-effects — those are exercised in the
//! kube-proxy integration tests.

use rusternetes_common::resources::{Service, ServicePort, ServiceSpec, ServiceType};

fn cluster_ip_service(name: &str, cluster_ip: Option<&str>) -> Service {
    Service::new(
        name,
        ServiceSpec {
            cluster_ip: cluster_ip.map(str::to_string),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: None,
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            ..Default::default()
        },
    )
}

// ---------------------------------------------------------------------------
// Headless service: clusterIP == "None"
// ---------------------------------------------------------------------------

#[test]
fn headless_service_keeps_cluster_ip_as_string_none() {
    // CoreDNS treats `clusterIP: "None"` as the headless marker — must
    // round-trip as the literal string "None", not be normalised away.
    let svc = cluster_ip_service("headless", Some("None"));
    let v = serde_json::to_value(&svc).unwrap();
    assert_eq!(v["spec"]["clusterIP"], "None");

    let decoded: Service = serde_json::from_value(v).unwrap();
    assert_eq!(decoded.spec.cluster_ip.as_deref(), Some("None"));
}

#[test]
fn cluster_ip_serializes_with_upper_case_ip() {
    // Pin the exact key name: must be `clusterIP`, not `clusterIp`.
    let svc = cluster_ip_service("a", Some("10.96.0.10"));
    let v = serde_json::to_value(&svc).unwrap();
    assert!(
        v["spec"].get("clusterIP").is_some(),
        "spec.clusterIP key required (camelCase abbreviation)"
    );
    assert!(
        v["spec"].get("clusterIp").is_none(),
        "lowercase `clusterIp` is not the K8s convention"
    );
}

// ---------------------------------------------------------------------------
// publishNotReadyAddresses — DNS records for headless / StatefulSet
// ---------------------------------------------------------------------------

#[test]
fn publish_not_ready_addresses_round_trips() {
    let svc = Service::new(
        "subset",
        ServiceSpec {
            cluster_ip: Some("None".to_string()),
            publish_not_ready_addresses: Some(true),
            ports: vec![],
            ..Default::default()
        },
    );
    let v = serde_json::to_value(&svc).unwrap();
    assert_eq!(v["spec"]["publishNotReadyAddresses"], true);

    let decoded: Service = serde_json::from_value(v).unwrap();
    assert_eq!(decoded.spec.publish_not_ready_addresses, Some(true));
}

// ---------------------------------------------------------------------------
// ExternalName service — DNS-only CNAME, no kube-proxy involvement
// ---------------------------------------------------------------------------

#[test]
fn external_name_service_carries_string_target() {
    let svc = Service::new(
        "ext",
        ServiceSpec {
            service_type: Some(ServiceType::ExternalName),
            external_name: Some("db.prod.example.com".to_string()),
            ports: vec![],
            ..Default::default()
        },
    );
    let v = serde_json::to_value(&svc).unwrap();
    assert_eq!(v["spec"]["type"], "ExternalName");
    assert_eq!(v["spec"]["externalName"], "db.prod.example.com");
}
