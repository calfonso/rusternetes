//! Create-side Pod validation tests — mirrors upstream Kubernetes v1.35.
//!
//! Sources:
//! - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/apis/core/validation/validation_test.go>
//!   (TestValidatePod, TestValidatePodSpec, TestValidateContainers,
//!   TestValidateInitContainers)
//!
//! Table-driven style: each case is a tuple `(&str label, Pod/Container/etc,
//! bool expect_valid)`. The test iterates and asserts `errs.is_empty()` for
//! valid cases and `!errs.is_empty()` for invalid ones, mirroring upstream's
//! `for _, tc := range successCases { ... }` / `for _, tc := range errorCases
//! { ... }` pattern.

use rusternetes_common::resources::pod::{
    Container, ContainerPort, Lifecycle, Pod, PodDNSConfig, PodSpec, Probe, Toleration,
};
use rusternetes_common::resources::pod::{ExecAction, HTTPGetAction, TCPSocketAction};
use rusternetes_common::resources::policy::IntOrString;
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_common::validation::pod::validate_pod_create;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pod_with_spec(spec: PodSpec) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some("default".to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(spec),
        status: None,
    }
}

fn minimal_spec(containers: Vec<Container>) -> PodSpec {
    PodSpec {
        containers,
        ..PodSpec::default()
    }
}

fn minimal_container(name: &str, image: &str) -> Container {
    Container {
        name: name.to_string(),
        image: image.to_string(),
        ..Container::default()
    }
}

fn check(pod: &Pod, expect_valid: bool, label: &str) {
    let errs = validate_pod_create(pod, true);
    if expect_valid {
        assert!(
            errs.is_empty(),
            "case {:?}: expected valid but got errors: {:?}",
            label,
            errs
        );
    } else {
        assert!(
            !errs.is_empty(),
            "case {:?}: expected errors but got none",
            label
        );
    }
}

fn contains_field(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field))
}

fn contains_detail(errs: &[rusternetes_common::validation::field::Error], detail: &str) -> bool {
    errs.iter()
        .any(|e| e.to_string().contains(detail) || e.detail.contains(detail))
}

// ---------------------------------------------------------------------------
// TestValidatePod - success cases
// Mirrors: pkg/apis/core/validation/validation_test.go TestValidatePod success cases
// ---------------------------------------------------------------------------

#[test]
fn test_validate_pod_success_minimal() {
    let pod = pod_with_spec(minimal_spec(vec![minimal_container("c", "nginx")]));
    let errs = validate_pod_create(&pod, true);
    assert!(errs.is_empty(), "minimal pod should be valid: {:?}", errs);
}

#[test]
fn test_validate_pod_success_multiple_containers() {
    let pod = pod_with_spec(minimal_spec(vec![
        minimal_container("c1", "nginx"),
        minimal_container("c2", "busybox"),
    ]));
    let errs = validate_pod_create(&pod, true);
    assert!(
        errs.is_empty(),
        "multi-container pod should be valid: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_success_with_init_containers() {
    let pod = pod_with_spec(PodSpec {
        init_containers: Some(vec![minimal_container("init-1", "busybox")]),
        containers: vec![minimal_container("c1", "nginx")],
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        errs.is_empty(),
        "pod with init container should be valid: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_success_restart_policy_never() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        restart_policy: Some("Never".to_string()),
        ..PodSpec::default()
    });
    check(&pod, true, "restartPolicy=Never");
}

#[test]
fn test_validate_pod_success_restart_policy_on_failure() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        restart_policy: Some("OnFailure".to_string()),
        ..PodSpec::default()
    });
    check(&pod, true, "restartPolicy=OnFailure");
}

#[test]
fn test_validate_pod_success_restart_policy_always() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        restart_policy: Some("Always".to_string()),
        ..PodSpec::default()
    });
    check(&pod, true, "restartPolicy=Always");
}

#[test]
fn test_validate_pod_success_active_deadline_seconds_positive() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        active_deadline_seconds: Some(1),
        ..PodSpec::default()
    });
    check(&pod, true, "activeDeadlineSeconds=1");
}

#[test]
fn test_validate_pod_success_termination_grace_period_zero() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        termination_grace_period_seconds: Some(0),
        ..PodSpec::default()
    });
    check(&pod, true, "terminationGracePeriodSeconds=0");
}

#[test]
fn test_validate_pod_success_termination_grace_period_positive() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        termination_grace_period_seconds: Some(30),
        ..PodSpec::default()
    });
    check(&pod, true, "terminationGracePeriodSeconds=30");
}

#[test]
fn test_validate_pod_success_dns_policy_cluster_first() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        dns_policy: Some("ClusterFirst".to_string()),
        ..PodSpec::default()
    });
    check(&pod, true, "dnsPolicy=ClusterFirst");
}

#[test]
fn test_validate_pod_success_dns_policy_default() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        dns_policy: Some("Default".to_string()),
        ..PodSpec::default()
    });
    check(&pod, true, "dnsPolicy=Default");
}

#[test]
fn test_validate_pod_success_dns_policy_none_with_config() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        dns_policy: Some("None".to_string()),
        dns_config: Some(PodDNSConfig {
            nameservers: Some(vec!["8.8.8.8".to_string()]),
            searches: None,
            options: None,
        }),
        ..PodSpec::default()
    });
    check(&pod, true, "dnsPolicy=None with config");
}

// ---------------------------------------------------------------------------
// TestValidatePod - error cases
// Mirrors: pkg/apis/core/validation/validation_test.go TestValidatePod error cases
// ---------------------------------------------------------------------------

#[test]
fn test_validate_pod_error_no_spec() {
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some("default".to_string()),
            ..ObjectMeta::default()
        },
        spec: None,
        status: None,
    };
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "nil spec should produce errors");
}

#[test]
fn test_validate_pod_error_empty_containers() {
    let pod = pod_with_spec(minimal_spec(vec![]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "empty containers should produce errors");
    assert!(
        contains_field(&errs, "containers"),
        "error should be on containers field: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_error_restart_policy_invalid() {
    let cases = vec!["always", "onfailure", "never", "Invalid", "ALWAYS"];
    for policy in cases {
        let pod = pod_with_spec(PodSpec {
            containers: vec![minimal_container("c", "nginx")],
            restart_policy: Some(policy.to_string()),
            ..PodSpec::default()
        });
        let errs = validate_pod_create(&pod, true);
        assert!(
            !errs.is_empty(),
            "restartPolicy={:?} should produce errors",
            policy
        );
        assert!(
            contains_field(&errs, "restartPolicy"),
            "error should be on restartPolicy: {:?}",
            errs
        );
    }
}

#[test]
fn test_validate_pod_error_active_deadline_seconds_zero() {
    // Mirrors upstream: activeDeadlineSeconds must be > 0.
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        active_deadline_seconds: Some(0),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "activeDeadlineSeconds=0 should fail");
    assert!(
        contains_field(&errs, "activeDeadlineSeconds"),
        "error on activeDeadlineSeconds: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_error_active_deadline_seconds_negative() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        active_deadline_seconds: Some(-1),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "activeDeadlineSeconds=-1 should fail");
}

#[test]
fn test_validate_pod_error_termination_grace_period_negative() {
    // Mirrors upstream: terminationGracePeriodSeconds must be >= 0.
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        termination_grace_period_seconds: Some(-1),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "terminationGracePeriodSeconds=-1 should fail"
    );
    assert!(
        contains_field(&errs, "terminationGracePeriodSeconds"),
        "error on terminationGracePeriodSeconds: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_error_ephemeral_containers_on_create() {
    use rusternetes_common::resources::pod::EphemeralContainer;
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        ephemeral_containers: Some(vec![EphemeralContainer {
            name: "dbg".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            env: None,
            volume_mounts: None,
            image_pull_policy: None,
            security_context: None,
            target_container_name: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            resize_policy: None,
            restart_policy: None,
            resources: None,
            termination_message_path: None,
            termination_message_policy: None,
            ..Default::default()
        }]),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "ephemeralContainers on create should fail"
    );
    assert!(
        contains_field(&errs, "ephemeralContainers"),
        "error on ephemeralContainers: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_error_dns_policy_invalid() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        dns_policy: Some("InvalidPolicy".to_string()),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "invalid dnsPolicy should fail");
    assert!(
        contains_field(&errs, "dnsPolicy"),
        "error on dnsPolicy: {:?}",
        errs
    );
}

#[test]
fn test_validate_pod_error_dns_policy_none_without_config() {
    // Mirrors: upstream requires dnsConfig when dnsPolicy is None.
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        dns_policy: Some("None".to_string()),
        dns_config: None,
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "dnsPolicy=None without dnsConfig should fail"
    );
    assert!(
        contains_field(&errs, "dnsConfig"),
        "error on dnsConfig: {:?}",
        errs
    );
}

// ---------------------------------------------------------------------------
// TestValidateContainers - success cases
// Mirrors: pkg/apis/core/validation/validation_test.go TestValidateContainers
// ---------------------------------------------------------------------------

#[test]
fn test_validate_containers_success_valid_name() {
    let cases = vec!["c", "c1", "c-1", "my-container", "c123"];
    for name in cases {
        let pod = pod_with_spec(minimal_spec(vec![minimal_container(name, "nginx")]));
        let errs = validate_pod_create(&pod, true);
        assert!(
            errs.is_empty(),
            "name {:?} should be valid: {:?}",
            name,
            errs
        );
    }
}

#[test]
fn test_validate_containers_success_with_ports() {
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        ports: Some(vec![ContainerPort {
            container_port: 80,
            name: Some("http".to_string()),
            protocol: Some("TCP".to_string()),
            host_port: None,
            host_ip: None,
        }]),
        ..Container::default()
    }]));
    check(&pod, true, "container with port");
}

#[test]
fn test_validate_containers_success_with_probe() {
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        liveness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/healthz".to_string()),
                port: IntOrString::Int(8080),
                host: None,
                scheme: None,
                http_headers: None,
            }),
            exec: None,
            tcp_socket: None,
            grpc: None,
            initial_delay_seconds: None,
            timeout_seconds: None,
            period_seconds: None,
            success_threshold: None,
            failure_threshold: None,
            termination_grace_period_seconds: None,
        }),
        ..Container::default()
    }]));
    check(&pod, true, "container with httpGet liveness probe");
}

#[test]
fn test_validate_containers_success_exec_probe() {
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        readiness_probe: Some(Probe {
            exec: Some(ExecAction {
                command: vec!["cat".to_string(), "/tmp/healthy".to_string()],
            }),
            http_get: None,
            tcp_socket: None,
            grpc: None,
            initial_delay_seconds: None,
            timeout_seconds: None,
            period_seconds: None,
            success_threshold: None,
            failure_threshold: None,
            termination_grace_period_seconds: None,
        }),
        ..Container::default()
    }]));
    check(&pod, true, "container with exec readiness probe");
}

#[test]
fn test_validate_containers_success_tcp_probe() {
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        liveness_probe: Some(Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(8080),
                host: None,
            }),
            http_get: None,
            exec: None,
            grpc: None,
            initial_delay_seconds: None,
            timeout_seconds: None,
            period_seconds: None,
            success_threshold: None,
            failure_threshold: None,
            termination_grace_period_seconds: None,
        }),
        ..Container::default()
    }]));
    check(&pod, true, "container with tcpSocket liveness probe");
}

#[test]
fn test_validate_containers_success_resources() {
    let mut limits = HashMap::new();
    limits.insert("cpu".to_string(), "500m".to_string());
    limits.insert("memory".to_string(), "128Mi".to_string());
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), "250m".to_string());
    requests.insert("memory".to_string(), "64Mi".to_string());

    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        resources: Some(ResourceRequirements {
            limits: Some(limits),
            requests: Some(requests),
            claims: None,
        }),
        ..Container::default()
    }]));
    check(&pod, true, "container with valid resources");
}

// ---------------------------------------------------------------------------
// TestValidateContainers - error cases
// ---------------------------------------------------------------------------

#[test]
fn test_validate_containers_error_empty_name() {
    let pod = pod_with_spec(minimal_spec(vec![minimal_container("", "nginx")]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "empty container name should fail");
    assert!(
        contains_field(&errs, "name"),
        "error on name field: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_invalid_name() {
    // Uppercase letters are not DNS-1123 label compliant.
    let cases = vec!["C", "my_container", "MyContainer", "my.container"];
    for name in cases {
        let pod = pod_with_spec(minimal_spec(vec![minimal_container(name, "nginx")]));
        let errs = validate_pod_create(&pod, true);
        assert!(!errs.is_empty(), "name {:?} should fail", name);
        assert!(
            contains_field(&errs, "name"),
            "error on name field for {:?}: {:?}",
            name,
            errs
        );
    }
}

#[test]
fn test_validate_containers_error_empty_image() {
    let pod = pod_with_spec(minimal_spec(vec![minimal_container("c", "")]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "empty image should fail");
    assert!(
        contains_field(&errs, "image"),
        "error on image field: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_duplicate_name_in_containers() {
    // Mirrors: upstream TestValidatePodSpec/duplicate_container_names
    let pod = pod_with_spec(minimal_spec(vec![
        minimal_container("ctr-a", "nginx"),
        minimal_container("ctr-a", "busybox"),
    ]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "duplicate container name should fail");
    assert!(contains_field(&errs, "name"), "error on name: {:?}", errs);
    assert!(
        contains_detail(&errs, "Duplicate"),
        "error should mention Duplicate: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_duplicate_name_across_init_and_regular() {
    let pod = pod_with_spec(PodSpec {
        init_containers: Some(vec![minimal_container("shared-name", "busybox")]),
        containers: vec![minimal_container("shared-name", "nginx")],
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "duplicate name across init and regular containers should fail"
    );
    assert!(
        contains_detail(&errs, "Duplicate"),
        "error should mention Duplicate: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_probe_no_handler() {
    // A probe with no handler set is invalid.
    // Mirrors: upstream TestValidateContainers probe without handler.
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        liveness_probe: Some(Probe {
            http_get: None,
            exec: None,
            tcp_socket: None,
            grpc: None,
            initial_delay_seconds: Some(5),
            timeout_seconds: None,
            period_seconds: None,
            success_threshold: None,
            failure_threshold: None,
            termination_grace_period_seconds: None,
        }),
        ..Container::default()
    }]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "probe with no handler should fail");
    assert!(
        contains_field(&errs, "livenessProbe"),
        "error on livenessProbe: {:?}",
        errs
    );
    assert!(
        contains_detail(&errs, "must specify a handler type"),
        "error detail should mention handler type: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_probe_multiple_handlers() {
    // A probe with more than one handler is invalid.
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        liveness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/".to_string()),
                port: IntOrString::Int(80),
                host: None,
                scheme: None,
                http_headers: None,
            }),
            exec: Some(ExecAction {
                command: vec!["true".to_string()],
            }),
            tcp_socket: None,
            grpc: None,
            initial_delay_seconds: None,
            timeout_seconds: None,
            period_seconds: None,
            success_threshold: None,
            failure_threshold: None,
            termination_grace_period_seconds: None,
        }),
        ..Container::default()
    }]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "probe with 2 handlers should fail");
}

#[test]
fn test_validate_containers_error_port_zero() {
    // containerPort = 0 is invalid.
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        ports: Some(vec![ContainerPort {
            container_port: 0,
            name: None,
            protocol: None,
            host_port: None,
            host_ip: None,
        }]),
        ..Container::default()
    }]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "containerPort=0 should fail");
    assert!(
        contains_field(&errs, "containerPort"),
        "error on containerPort: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_port_protocol_invalid() {
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        ports: Some(vec![ContainerPort {
            container_port: 80,
            name: None,
            protocol: Some("INVALID".to_string()),
            host_port: None,
            host_ip: None,
        }]),
        ..Container::default()
    }]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "invalid protocol should fail");
    assert!(
        contains_field(&errs, "protocol"),
        "error on protocol: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_duplicate_port_name() {
    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        ports: Some(vec![
            ContainerPort {
                container_port: 80,
                name: Some("http".to_string()),
                protocol: None,
                host_port: None,
                host_ip: None,
            },
            ContainerPort {
                container_port: 8080,
                name: Some("http".to_string()),
                protocol: None,
                host_port: None,
                host_ip: None,
            },
        ]),
        ..Container::default()
    }]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "duplicate port name should fail");
    assert!(
        contains_detail(&errs, "Duplicate"),
        "error should mention Duplicate: {:?}",
        errs
    );
}

#[test]
fn test_validate_containers_error_resources_request_exceeds_limit() {
    // Mirrors: upstream TestValidateContainers requests > limits.
    let mut limits = HashMap::new();
    limits.insert("cpu".to_string(), "100m".to_string());
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), "200m".to_string()); // request > limit

    let pod = pod_with_spec(minimal_spec(vec![Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        resources: Some(ResourceRequirements {
            limits: Some(limits),
            requests: Some(requests),
            claims: None,
        }),
        ..Container::default()
    }]));
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "request > limit should fail");
    assert!(
        contains_field(&errs, "requests"),
        "error on resources.requests: {:?}",
        errs
    );
}

// ---------------------------------------------------------------------------
// TestValidateInitContainers
// Mirrors: pkg/apis/core/validation/validation_test.go TestValidateInitContainers
// ---------------------------------------------------------------------------

#[test]
fn test_validate_init_containers_error_readiness_probe_forbidden() {
    // Init containers may not have readinessProbe.
    // Mirrors: upstream TestValidateInitContainers readinessProbe error.
    let pod = pod_with_spec(PodSpec {
        init_containers: Some(vec![Container {
            name: "init".to_string(),
            image: "busybox".to_string(),
            readiness_probe: Some(Probe {
                exec: Some(ExecAction {
                    command: vec!["true".to_string()],
                }),
                http_get: None,
                tcp_socket: None,
                grpc: None,
                initial_delay_seconds: None,
                timeout_seconds: None,
                period_seconds: None,
                success_threshold: None,
                failure_threshold: None,
                termination_grace_period_seconds: None,
            }),
            ..Container::default()
        }]),
        containers: vec![minimal_container("c", "nginx")],
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "init container with readinessProbe should fail"
    );
    assert!(
        contains_field(&errs, "readinessProbe"),
        "error on readinessProbe: {:?}",
        errs
    );
    assert!(
        contains_detail(&errs, "must not be set for init containers"),
        "error detail: {:?}",
        errs
    );
}

#[test]
fn test_validate_restartable_init_container_allows_readiness_probe() {
    // A restartable init container (sidecar: restartPolicy=Always) MAY have a
    // readinessProbe — unlike a plain init container. Mirrors upstream
    // validateInitContainers (forbidden only "without restartPolicy=Always").
    let pod = pod_with_spec(PodSpec {
        init_containers: Some(vec![Container {
            name: "sidecar".to_string(),
            image: "busybox".to_string(),
            restart_policy: Some("Always".to_string()),
            readiness_probe: Some(Probe {
                exec: Some(ExecAction {
                    command: vec!["true".to_string()],
                }),
                http_get: None,
                tcp_socket: None,
                grpc: None,
                initial_delay_seconds: None,
                timeout_seconds: None,
                period_seconds: None,
                success_threshold: None,
                failure_threshold: None,
                termination_grace_period_seconds: None,
            }),
            ..Container::default()
        }]),
        containers: vec![minimal_container("c", "nginx")],
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !contains_detail(&errs, "must not be set for init containers"),
        "restartable init container readinessProbe must be allowed: {:?}",
        errs
    );
}

#[test]
fn test_validate_init_containers_error_lifecycle_forbidden() {
    use rusternetes_common::resources::pod::LifecycleHandler;
    // Init containers may not have lifecycle hooks.
    let pod = pod_with_spec(PodSpec {
        init_containers: Some(vec![Container {
            name: "init".to_string(),
            image: "busybox".to_string(),
            lifecycle: Some(Lifecycle {
                post_start: Some(LifecycleHandler {
                    exec: Some(ExecAction {
                        command: vec!["true".to_string()],
                    }),
                    http_get: None,
                    tcp_socket: None,
                    sleep: None,
                }),
                pre_stop: None,
                stop_signal: None,
            }),
            ..Container::default()
        }]),
        containers: vec![minimal_container("c", "nginx")],
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "init container with lifecycle should fail"
    );
    assert!(
        contains_field(&errs, "lifecycle"),
        "error on lifecycle: {:?}",
        errs
    );
    assert!(
        contains_detail(&errs, "must not be set for init containers"),
        "error detail: {:?}",
        errs
    );
}

#[test]
fn test_validate_init_containers_success_liveness_probe_allowed() {
    // Init containers may have livenessProbe.
    let pod = pod_with_spec(PodSpec {
        init_containers: Some(vec![Container {
            name: "init".to_string(),
            image: "busybox".to_string(),
            liveness_probe: Some(Probe {
                exec: Some(ExecAction {
                    command: vec!["true".to_string()],
                }),
                http_get: None,
                tcp_socket: None,
                grpc: None,
                initial_delay_seconds: None,
                timeout_seconds: None,
                period_seconds: None,
                success_threshold: None,
                failure_threshold: None,
                termination_grace_period_seconds: None,
            }),
            ..Container::default()
        }]),
        containers: vec![minimal_container("c", "nginx")],
        ..PodSpec::default()
    });
    check(&pod, true, "init container with livenessProbe is valid");
}

// ---------------------------------------------------------------------------
// TestValidatePodSpec - volumes
// ---------------------------------------------------------------------------

#[test]
fn test_validate_pod_spec_volumes_success_unique_names() {
    use rusternetes_common::resources::pod::{EmptyDirVolumeSource, Volume};
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        volumes: Some(vec![
            Volume {
                name: "vol-a".to_string(),
                empty_dir: Some(EmptyDirVolumeSource {
                    medium: None,
                    size_limit: None,
                }),
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
                image: None,
            },
            Volume {
                name: "vol-b".to_string(),
                empty_dir: Some(EmptyDirVolumeSource {
                    medium: None,
                    size_limit: None,
                }),
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
                image: None,
            },
        ]),
        ..PodSpec::default()
    });
    check(&pod, true, "unique volume names");
}

#[test]
fn test_validate_pod_spec_volumes_error_duplicate_names() {
    use rusternetes_common::resources::pod::{EmptyDirVolumeSource, Volume};
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        volumes: Some(vec![
            Volume {
                name: "vol-a".to_string(),
                empty_dir: Some(EmptyDirVolumeSource {
                    medium: None,
                    size_limit: None,
                }),
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
                image: None,
            },
            Volume {
                name: "vol-a".to_string(),
                empty_dir: Some(EmptyDirVolumeSource {
                    medium: None,
                    size_limit: None,
                }),
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
                image: None,
            },
        ]),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "duplicate volume name should fail");
    assert!(
        contains_detail(&errs, "Duplicate"),
        "error should mention Duplicate: {:?}",
        errs
    );
}

// ---------------------------------------------------------------------------
// Tolerations
// ---------------------------------------------------------------------------

#[test]
fn test_validate_tolerations_success() {
    let cases: Vec<(&str, Toleration)> = vec![
        (
            "exists operator no value",
            Toleration {
                key: Some("foo".to_string()),
                operator: Some("Exists".to_string()),
                value: None,
                effect: None,
                toleration_seconds: None,
            },
        ),
        (
            "equal operator with value",
            Toleration {
                key: Some("foo".to_string()),
                operator: Some("Equal".to_string()),
                value: Some("bar".to_string()),
                effect: None,
                toleration_seconds: None,
            },
        ),
        (
            "no execute with toleration seconds",
            Toleration {
                key: Some("foo".to_string()),
                operator: Some("Equal".to_string()),
                value: Some("bar".to_string()),
                effect: Some("NoExecute".to_string()),
                toleration_seconds: Some(60),
            },
        ),
        (
            "no schedule effect",
            Toleration {
                key: Some("foo".to_string()),
                operator: None,
                value: None,
                effect: Some("NoSchedule".to_string()),
                toleration_seconds: None,
            },
        ),
    ];

    for (label, tol) in cases {
        let pod = pod_with_spec(PodSpec {
            containers: vec![minimal_container("c", "nginx")],
            tolerations: Some(vec![tol]),
            ..PodSpec::default()
        });
        check(&pod, true, label);
    }
}

#[test]
fn test_validate_tolerations_error_invalid_operator() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        tolerations: Some(vec![Toleration {
            key: Some("foo".to_string()),
            operator: Some("InvalidOp".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }]),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "invalid toleration operator should fail");
    assert!(
        contains_field(&errs, "operator"),
        "error on operator: {:?}",
        errs
    );
}

#[test]
fn test_validate_tolerations_error_invalid_effect() {
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        tolerations: Some(vec![Toleration {
            key: Some("foo".to_string()),
            operator: None,
            value: None,
            effect: Some("InvalidEffect".to_string()),
            toleration_seconds: None,
        }]),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "invalid toleration effect should fail");
    assert!(
        contains_field(&errs, "effect"),
        "error on effect: {:?}",
        errs
    );
}

#[test]
fn test_validate_tolerations_error_exists_with_value() {
    // Exists operator must not have a value.
    // Mirrors upstream: validation_test.go tolerations error cases.
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        tolerations: Some(vec![Toleration {
            key: Some("foo".to_string()),
            operator: Some("Exists".to_string()),
            value: Some("bar".to_string()),
            effect: None,
            toleration_seconds: None,
        }]),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(!errs.is_empty(), "Exists operator with value should fail");
    assert!(
        contains_field(&errs, "operator"),
        "error on operator: {:?}",
        errs
    );
}

#[test]
fn test_validate_tolerations_error_toleration_seconds_without_no_execute() {
    // tolerationSeconds requires effect=NoExecute.
    let pod = pod_with_spec(PodSpec {
        containers: vec![minimal_container("c", "nginx")],
        tolerations: Some(vec![Toleration {
            key: Some("foo".to_string()),
            operator: None,
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: Some(30),
        }]),
        ..PodSpec::default()
    });
    let errs = validate_pod_create(&pod, true);
    assert!(
        !errs.is_empty(),
        "tolerationSeconds with NoSchedule should fail"
    );
    assert!(
        contains_field(&errs, "tolerationSeconds"),
        "error on tolerationSeconds: {:?}",
        errs
    );
}

#[test]
fn host_network_requires_hostport_matches_containerport() {
    let make = |host_network: Option<bool>, host_port: Option<u16>| {
        let mut c = minimal_container("app", "busybox");
        c.ports = Some(vec![ContainerPort {
            container_port: 80,
            name: None,
            // Protocol is TCP-defaulted before validation runs; supply it so the
            // fixture reflects the post-defaulting state (validateContainerPorts
            // requires a non-empty protocol, mirroring upstream).
            protocol: Some("TCP".to_string()),
            host_port,
            host_ip: None,
        }]);
        let mut spec = minimal_spec(vec![c]);
        spec.host_network = host_network;
        pod_with_spec(spec)
    };

    // hostNetwork=true + unset (0) hostPort != containerPort 80 -> rejected on
    // the hostPort field with the upstream message.
    let pod = make(Some(true), None);
    let errs = validate_pod_create(&pod, true);
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.containers[0].ports[0].hostPort"
                && e.detail
                    .contains("must match `containerPort` when `hostNetwork` is true")),
        "expected hostPort-mismatch error, got: {errs:?}"
    );

    // hostNetwork=true + matching hostPort -> accepted.
    check(
        &make(Some(true), Some(80)),
        true,
        "hostNetwork with matching hostPort",
    );

    // hostNetwork=true + mismatched non-zero hostPort -> rejected.
    check(
        &make(Some(true), Some(8080)),
        false,
        "hostNetwork with mismatched hostPort",
    );

    // No hostNetwork -> the rule does not apply even with an unset hostPort.
    check(&make(None, None), true, "no hostNetwork, unset hostPort");
}
