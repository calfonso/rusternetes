//! Table-driven tests for Service field validation.
//!
//! Mirrors the semantics of upstream Kubernetes `TestValidateService*` in
//! `pkg/apis/core/validation/validation_test.go` (release-1.35).
//!
//! Upstream source:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/apis/core/validation/validation_test.go>

#![allow(clippy::useless_vec)]

use rusternetes_common::resources::policy::IntOrString;
use rusternetes_common::resources::service::{
    ClientIPConfig, Service, ServiceExternalTrafficPolicy, ServicePort, ServiceSpec, ServiceType,
    SessionAffinityConfig,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::service::validate_service;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn make_service(spec: ServiceSpec) -> Service {
    Service::new("test-service", spec)
}

fn single_port(port: u16, protocol: &str) -> Vec<ServicePort> {
    vec![ServicePort {
        name: None,
        port,
        target_port: None,
        protocol: protocol.to_string(),
        node_port: None,
        app_protocol: None,
    }]
}

fn named_port(name: &str, port: u16, protocol: &str) -> ServicePort {
    ServicePort {
        name: Some(name.to_string()),
        port,
        target_port: None,
        protocol: protocol.to_string(),
        node_port: None,
        app_protocol: None,
    }
}

fn aggregate_errors(svc: &Service) -> Vec<String> {
    validate_service(svc)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// TestValidateService — valid cases
// ---------------------------------------------------------------------------

/// A minimal ClusterIP service with a single port should pass validation.
#[test]
fn test_validate_service_valid_minimal() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// A NodePort service with a valid nodePort should pass.
#[test]
fn test_validate_service_valid_nodeport() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::NodePort),
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: Some(30080),
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// An ExternalName service with a valid externalName should pass.
#[test]
fn test_validate_service_valid_external_name() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ExternalName),
        external_name: Some("my.example.com".to_string()),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Multi-port service with distinct named ports should pass.
#[test]
fn test_validate_service_valid_multi_port() {
    let svc = make_service(ServiceSpec {
        ports: vec![
            named_port("http", 80, "TCP"),
            named_port("https", 443, "TCP"),
        ],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Service with externalIPs set to valid IPs should pass.
#[test]
fn test_validate_service_valid_external_ips() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        external_ips: Some(vec!["1.2.3.4".to_string(), "5.6.7.8".to_string()]),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Service with sessionAffinity=ClientIP and a valid timeout should pass.
#[test]
fn test_validate_service_valid_session_affinity_clientip() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        session_affinity: Some("ClientIP".to_string()),
        session_affinity_config: Some(SessionAffinityConfig {
            client_ip: Some(ClientIPConfig {
                timeout_seconds: Some(10800),
            }),
        }),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Boundary: sessionAffinity timeout at minimum (1 second) should pass.
#[test]
fn test_validate_service_valid_session_affinity_min_timeout() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        session_affinity: Some("ClientIP".to_string()),
        session_affinity_config: Some(SessionAffinityConfig {
            client_ip: Some(ClientIPConfig {
                timeout_seconds: Some(1),
            }),
        }),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Boundary: sessionAffinity timeout at maximum (86400 seconds) should pass.
#[test]
fn test_validate_service_valid_session_affinity_max_timeout() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        session_affinity: Some("ClientIP".to_string()),
        session_affinity_config: Some(SessionAffinityConfig {
            client_ip: Some(ClientIPConfig {
                timeout_seconds: Some(86400),
            }),
        }),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// LoadBalancer service with healthCheckNodePort in range should pass.
#[test]
fn test_validate_service_valid_health_check_node_port() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::LoadBalancer),
        ports: single_port(80, "TCP"),
        health_check_node_port: Some(31000),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Service with clusterIP="None" (headless) should pass.
#[test]
fn test_validate_service_valid_headless() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("None".to_string()),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Service with a valid clusterIP address should pass.
#[test]
fn test_validate_service_valid_cluster_ip() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("10.96.0.10".to_string()),
        // A non-headless ClusterIP service must declare at least one port
        // (upstream `ValidateService`).
        ports: single_port(80, "TCP"),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

// ---------------------------------------------------------------------------
// TestValidateService — invalid cases (port number)
// ---------------------------------------------------------------------------

/// Port 0 should fail validation.
#[test]
fn test_validate_service_invalid_port_zero() {
    let svc = make_service(ServiceSpec {
        ports: single_port(0, "TCP"),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("spec.ports[0].port")),
        "expected port error, got: {:?}",
        errors
    );
}

/// Port boundary value 65535 should pass.
#[test]
fn test_validate_service_valid_port_max() {
    let svc = make_service(ServiceSpec {
        ports: single_port(65535, "TCP"),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Port 1 (minimum) should pass.
#[test]
fn test_validate_service_valid_port_min() {
    let svc = make_service(ServiceSpec {
        ports: single_port(1, "TCP"),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

// ---------------------------------------------------------------------------
// TestValidateService — invalid protocol
// ---------------------------------------------------------------------------

/// Unknown protocol should fail with NotSupported.
#[test]
fn test_validate_service_invalid_protocol() {
    let svc = make_service(ServiceSpec {
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "UNKNOWN".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.ports[0].protocol") && e.contains("Unsupported")),
        "expected unsupported protocol error, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — targetPort
// ---------------------------------------------------------------------------

/// Numeric targetPort=0 should fail.
#[test]
fn test_validate_service_invalid_target_port_zero() {
    let svc = make_service(ServiceSpec {
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: Some(IntOrString::Int(0)),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.ports[0].targetPort")),
        "expected targetPort error, got: {:?}",
        errors
    );
}

/// Invalid string targetPort (not a valid port name) should fail.
#[test]
fn test_validate_service_invalid_target_port_bad_name() {
    let svc = make_service(ServiceSpec {
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: Some(IntOrString::String("Bad_Port_Name!".to_string())),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.ports[0].targetPort")),
        "expected targetPort error, got: {:?}",
        errors
    );
}

/// Valid string targetPort should pass.
#[test]
fn test_validate_service_valid_target_port_name() {
    let svc = make_service(ServiceSpec {
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: Some(IntOrString::String("http".to_string())),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

// ---------------------------------------------------------------------------
// TestValidateService — nodePort
// ---------------------------------------------------------------------------

/// NodePort below range (29999) on NodePort service type should fail.
#[test]
fn test_validate_service_nodeport_too_low() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::NodePort),
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: Some(29999),
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("spec.ports[0].nodePort")),
        "expected nodePort out-of-range error, got: {:?}",
        errors
    );
}

/// NodePort above range (32768) on NodePort service type should fail.
#[test]
fn test_validate_service_nodeport_too_high() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::NodePort),
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: Some(32768),
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("spec.ports[0].nodePort")),
        "expected nodePort out-of-range error, got: {:?}",
        errors
    );
}

/// NodePort on ClusterIP service is forbidden.
#[test]
fn test_validate_service_nodeport_on_clusterip_forbidden() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ClusterIP),
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: Some(30000),
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.ports[0].nodePort") && e.contains("Forbidden")),
        "expected Forbidden nodePort error, got: {:?}",
        errors
    );
}

/// Boundary nodePort=30000 on NodePort service should pass.
#[test]
fn test_validate_service_nodeport_at_min() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::NodePort),
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: Some(30000),
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

/// Boundary nodePort=32767 on NodePort service should pass.
#[test]
fn test_validate_service_nodeport_at_max() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::NodePort),
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: Some(32767),
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
}

// ---------------------------------------------------------------------------
// TestValidateService — duplicate ports
// ---------------------------------------------------------------------------

/// Two ports with same (port, protocol) should produce a Duplicate error.
#[test]
fn test_validate_service_duplicate_port_proto() {
    let svc = make_service(ServiceSpec {
        ports: vec![
            named_port("http", 80, "TCP"),
            named_port("http-alt", 80, "TCP"),
        ],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("Duplicate") && e.contains("port")),
        "expected Duplicate port error, got: {:?}",
        errors
    );
}

/// Two ports with the same name should produce a Duplicate error.
#[test]
fn test_validate_service_duplicate_port_name() {
    let svc = make_service(ServiceSpec {
        ports: vec![
            named_port("http", 80, "TCP"),
            named_port("http", 8080, "TCP"),
        ],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("Duplicate") && e.contains("name")),
        "expected Duplicate name error, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — multi-port requires names
// ---------------------------------------------------------------------------

/// Multi-port service without names should require names.
#[test]
fn test_validate_service_multi_port_requires_names() {
    let svc = make_service(ServiceSpec {
        ports: vec![
            ServicePort {
                name: None,
                port: 80,
                target_port: None,
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            },
            ServicePort {
                name: None,
                port: 443,
                target_port: None,
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            },
        ],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("Required") && e.contains("name")),
        "expected Required name error for multi-port, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — ExternalName
// ---------------------------------------------------------------------------

/// ExternalName service without externalName field should fail.
#[test]
fn test_validate_service_external_name_required() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ExternalName),
        external_name: None,
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.externalName") && e.contains("Required")),
        "expected Required externalName error, got: {:?}",
        errors
    );
}

/// ExternalName service with invalid DNS name should fail.
#[test]
fn test_validate_service_external_name_invalid_dns() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ExternalName),
        external_name: Some("not_a_valid_dns!".to_string()),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("spec.externalName")),
        "expected externalName validation error, got: {:?}",
        errors
    );
}

/// Non-ExternalName service with externalName set should fail with Forbidden.
#[test]
fn test_validate_service_external_name_forbidden_on_clusterip() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ClusterIP),
        external_name: Some("example.com".to_string()),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.externalName") && e.contains("Forbidden")),
        "expected Forbidden externalName error, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — externalIPs
// ---------------------------------------------------------------------------

/// Invalid externalIP (not an IP) should fail.
#[test]
fn test_validate_service_invalid_external_ip() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        external_ips: Some(vec!["not-an-ip".to_string()]),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("spec.externalIPs[0]")),
        "expected externalIPs invalid error, got: {:?}",
        errors
    );
}

/// Duplicate externalIP should fail.
#[test]
fn test_validate_service_duplicate_external_ip() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        external_ips: Some(vec!["1.2.3.4".to_string(), "1.2.3.4".to_string()]),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.externalIPs") && e.contains("Duplicate")),
        "expected Duplicate externalIPs error, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — sessionAffinity
// ---------------------------------------------------------------------------

/// Invalid sessionAffinity value should produce NotSupported.
#[test]
fn test_validate_service_invalid_session_affinity() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        session_affinity: Some("Invalid".to_string()),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spec.sessionAffinity") && e.contains("Unsupported")),
        "expected Unsupported sessionAffinity error, got: {:?}",
        errors
    );
}

/// Timeout below minimum (0) should fail.
#[test]
fn test_validate_service_session_affinity_timeout_too_low() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        session_affinity: Some("ClientIP".to_string()),
        session_affinity_config: Some(SessionAffinityConfig {
            client_ip: Some(ClientIPConfig {
                timeout_seconds: Some(0),
            }),
        }),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("timeoutSeconds")),
        "expected timeoutSeconds error for value=0, got: {:?}",
        errors
    );
}

/// Timeout above maximum (86401) should fail.
#[test]
fn test_validate_service_session_affinity_timeout_too_high() {
    let svc = make_service(ServiceSpec {
        ports: single_port(80, "TCP"),
        session_affinity: Some("ClientIP".to_string()),
        session_affinity_config: Some(SessionAffinityConfig {
            client_ip: Some(ClientIPConfig {
                timeout_seconds: Some(86401),
            }),
        }),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("timeoutSeconds")),
        "expected timeoutSeconds error for value=86401, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — healthCheckNodePort
// ---------------------------------------------------------------------------

/// healthCheckNodePort on ClusterIP service should fail.
#[test]
fn test_validate_service_health_check_node_port_on_clusterip() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ClusterIP),
        ports: single_port(80, "TCP"),
        health_check_node_port: Some(31000),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("healthCheckNodePort") && e.contains("Forbidden")),
        "expected Forbidden healthCheckNodePort error, got: {:?}",
        errors
    );
}

/// healthCheckNodePort below range on LoadBalancer should fail.
#[test]
fn test_validate_service_health_check_node_port_too_low() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::LoadBalancer),
        ports: single_port(80, "TCP"),
        health_check_node_port: Some(1000),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("healthCheckNodePort")),
        "expected healthCheckNodePort range error, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — invalid clusterIP
// ---------------------------------------------------------------------------

/// Invalid clusterIP (not an IP and not "None") should fail.
#[test]
fn test_validate_service_invalid_cluster_ip() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("not-an-ip".to_string()),
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.iter().any(|e| e.contains("spec.clusterIP")),
        "expected clusterIP invalid error, got: {:?}",
        errors
    );
}

// ---------------------------------------------------------------------------
// TestValidateService — port name format
// ---------------------------------------------------------------------------

/// A 25-char port name that is a valid DNS-1123 label is ACCEPTED. Upstream
/// validates ServicePort.Name with ValidateDNS1123Label (≤63 chars), not the
/// 15-char IsValidPortName rule (which applies to the string targetPort).
#[test]
fn test_validate_service_port_name_long_dns_label_ok() {
    let svc = make_service(ServiceSpec {
        ports: vec![named_port("this-name-is-way-too-long", 80, "TCP")],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.is_empty(),
        "25-char DNS-label port name should be valid, got: {:?}",
        errors
    );
}

/// An all-digit port name ("12345") is ACCEPTED — a valid DNS-1123 label does
/// not require a letter (unlike IsValidPortName / IANA_SVC_NAME).
#[test]
fn test_validate_service_port_name_all_digits_ok() {
    let svc = make_service(ServiceSpec {
        ports: vec![named_port("12345", 80, "TCP")],
        ..ServiceSpec::default()
    });
    let errors = aggregate_errors(&svc);
    assert!(
        errors.is_empty(),
        "all-digit DNS-label port name should be valid, got: {:?}",
        errors
    );
}

/// Valid port names should pass.
#[test]
fn test_validate_service_port_name_valid() {
    let valid_names = vec!["http", "https", "h2c", "grpc-web", "my-port"];
    for name in valid_names {
        let svc = make_service(ServiceSpec {
            ports: vec![named_port(name, 80, "TCP")],
            ..ServiceSpec::default()
        });
        let errs = validate_service(&svc);
        assert!(
            errs.iter().all(|e| !e.field.contains("name")),
            "unexpected name error for {:?}: {:?}",
            name,
            errs
        );
    }
}

// ---------------------------------------------------------------------------
// TestValidateService — all valid protocols
// ---------------------------------------------------------------------------

/// UDP and SCTP ports should pass.
#[test]
fn test_validate_service_udp_sctp_protocols() {
    for proto in &["UDP", "SCTP"] {
        let svc = make_service(ServiceSpec {
            ports: single_port(80, proto),
            ..ServiceSpec::default()
        });
        let errs = validate_service(&svc);
        assert!(
            errs.iter().all(|e| !e.field.contains("protocol")),
            "unexpected protocol error for {}: {:?}",
            proto,
            errs
        );
    }
}

// ---------------------------------------------------------------------------
// ServicePort.Name is a DNS-1123 label (≤63), NOT IsValidPortName (≤15)
//
// Upstream validates ServicePort.Name with ValidateDNS1123Label; only the
// *string* targetPort uses the 15-char IANA_SVC_NAME rule. Regression for the
// cert-manager smoke test, whose Service ships a 29-char port name.
// ---------------------------------------------------------------------------

/// A Service port named `tcp-prometheus-servicemonitor` (29 chars, a valid
/// DNS-1123 label) must be accepted — it is rejected by the 15-char rule but
/// allowed upstream, and blocked cert-manager's install.
#[test]
fn test_validate_service_long_dns_label_port_name_ok() {
    let svc = make_service(ServiceSpec {
        ports: vec![
            named_port("tcp-prometheus-servicemonitor", 9402, "TCP"),
            named_port("https", 443, "TCP"),
        ],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        errs.is_empty(),
        "29-char DNS-label port name should be valid: {errs:?}"
    );
}

/// A port name that is not a valid DNS-1123 label (uppercase) is still rejected.
#[test]
fn test_validate_service_invalid_port_name_rejected() {
    let svc = make_service(ServiceSpec {
        ports: vec![named_port("Bad_Name", 80, "TCP")],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs.is_empty(),
        "uppercase/underscore port name should be rejected"
    );
}

/// A port name longer than 63 chars (the DNS-1123 label max) is rejected.
#[test]
fn test_validate_service_port_name_over_63_rejected() {
    let long = "a".repeat(64);
    let svc = make_service(ServiceSpec {
        ports: vec![named_port(&long, 80, "TCP")],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs.is_empty(),
        "64-char port name should exceed the DNS-1123 label max"
    );
}

/// A *string* targetPort keeps the 15-char IANA_SVC_NAME limit (must match a
/// container port name), so a 29-char string targetPort is still rejected.
#[test]
fn test_validate_service_string_targetport_keeps_15_char_limit() {
    let svc = make_service(ServiceSpec {
        ports: vec![ServicePort {
            name: Some("web".to_string()),
            port: 80,
            target_port: Some(IntOrString::String(
                "tcp-prometheus-servicemonitor".to_string(),
            )),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs.is_empty(),
        "29-char string targetPort should still be rejected (IANA_SVC_NAME)"
    );
}

// ---------------------------------------------------------------------------
// ports-required + externalTrafficPolicy field restrictions
// ---------------------------------------------------------------------------

/// A non-headless ClusterIP service with no ports is rejected.
#[test]
fn test_clusterip_requires_ports() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("10.96.0.10".to_string()),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.ports" && e.error_type == ErrorType::Required),
        "expected spec.ports Required, got: {errs:?}"
    );
}

/// A headless service (clusterIP "None") may legitimately have no ports.
#[test]
fn test_headless_allows_no_ports() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("None".to_string()),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs
            .iter()
            .any(|e| e.field == "spec.ports" && e.error_type == ErrorType::Required),
        "headless service should not require ports, got: {errs:?}"
    );
}

/// An ExternalName service may have no ports.
#[test]
fn test_externalname_allows_no_ports() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::ExternalName),
        external_name: Some("example.com".to_string()),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs
            .iter()
            .any(|e| e.field == "spec.ports" && e.error_type == ErrorType::Required),
        "ExternalName service should not require ports, got: {errs:?}"
    );
}

/// externalTrafficPolicy on a plain ClusterIP service (no externalIPs) is
/// rejected — it may only be set for externally-accessible services.
#[test]
fn test_etp_forbidden_on_clusterip() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("10.96.0.10".to_string()),
        ports: single_port(80, "TCP"),
        external_traffic_policy: Some(ServiceExternalTrafficPolicy::Local),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.externalTrafficPolicy" && e.error_type == ErrorType::Invalid),
        "expected externalTrafficPolicy Invalid on ClusterIP, got: {errs:?}"
    );
}

/// externalTrafficPolicy is allowed on a NodePort service.
#[test]
fn test_etp_allowed_on_nodeport() {
    let svc = make_service(ServiceSpec {
        service_type: Some(ServiceType::NodePort),
        ports: single_port(80, "TCP"),
        external_traffic_policy: Some(ServiceExternalTrafficPolicy::Local),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs.iter().any(|e| e.field == "spec.externalTrafficPolicy"),
        "externalTrafficPolicy should be allowed on NodePort, got: {errs:?}"
    );
}

/// externalTrafficPolicy is allowed on a ClusterIP service that has externalIPs.
#[test]
fn test_etp_allowed_on_clusterip_with_external_ips() {
    let svc = make_service(ServiceSpec {
        cluster_ip: Some("10.96.0.10".to_string()),
        ports: single_port(80, "TCP"),
        external_ips: Some(vec!["1.2.3.4".to_string()]),
        external_traffic_policy: Some(ServiceExternalTrafficPolicy::Cluster),
        ..ServiceSpec::default()
    });
    let errs = validate_service(&svc);
    assert!(
        !errs.iter().any(|e| e.field == "spec.externalTrafficPolicy"),
        "externalTrafficPolicy should be allowed on ClusterIP+externalIPs, got: {errs:?}"
    );
}
