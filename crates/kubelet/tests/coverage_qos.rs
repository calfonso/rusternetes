//! Unit tests for Pod QoS class determination.
//!
//! These tests pin the behaviour of [`rusternetes_kubelet::eviction::get_qos_class`],
//! which mirrors upstream Kubernetes' `ComputePodQOS` in
//! <https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/qos/qos.go>
//! (test suite at
//! <https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/qos/qos_test.go>).
//!
//! `get_qos_class` classifies a pod into one of three QoS classes, inspecting
//! both the regular containers in `spec.containers` **and** the init containers
//! in `spec.init_containers` (ephemeral containers are excluded — they cannot
//! declare resources):
//!
//! - **Guaranteed** – every container has explicit CPU **and** memory limits,
//!   and its effective requests equal those limits for every resource. A
//!   container that sets limits but no requests is treated as if its requests
//!   equal its limits (upstream defaults a missing request to the limit).
//! - **Burstable** – at least one container has some resource request or limit,
//!   but the pod does not qualify as Guaranteed.
//! - **BestEffort** – no container has any resource requests or limits.

use rusternetes_common::resources::{Container, EphemeralContainer, PodSpec};
use rusternetes_common::resources::{Pod, PodStatus};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_kubelet::eviction::{get_qos_class, QoSClass};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Builder helpers
// ---------------------------------------------------------------------------

/// Minimal `Container` with only the name and image set; all other fields are
/// `None`.  Use [`with_resources`] to attach resource requirements.
fn make_container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "test-image:latest".to_string(),
        resources: None,
        image_pull_policy: None,
        command: None,
        args: None,
        ports: None,
        env: None,
        volume_mounts: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        working_dir: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        env_from: None,
        volume_devices: None,
        ..Default::default()
    }
}

/// Attach `ResourceRequirements` to a `Container`.
fn with_resources(mut c: Container, r: ResourceRequirements) -> Container {
    c.resources = Some(r);
    c
}

/// Build a `ResourceRequirements` where both `requests` and `limits` contain
/// the same cpu/memory pair (the Guaranteed pattern).
fn guaranteed_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    let map = HashMap::from([
        ("cpu".to_string(), cpu.to_string()),
        ("memory".to_string(), memory.to_string()),
    ]);
    ResourceRequirements {
        requests: Some(map.clone()),
        limits: Some(map),
        claims: None,
    }
}

/// Build a `ResourceRequirements` with only limits set (no explicit requests).
///
/// `get_qos_class` defaults the missing request to the matching limit before
/// classifying, so a limits-only container (cpu + memory limits) is
/// Guaranteed, matching upstream.
fn limits_only_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    ResourceRequirements {
        requests: None,
        limits: Some(HashMap::from([
            ("cpu".to_string(), cpu.to_string()),
            ("memory".to_string(), memory.to_string()),
        ])),
        claims: None,
    }
}

/// Build a `ResourceRequirements` with mismatched requests vs limits
/// (classic Burstable pattern).
fn burstable_resources(
    req_cpu: &str,
    req_mem: &str,
    lim_cpu: &str,
    lim_mem: &str,
) -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(HashMap::from([
            ("cpu".to_string(), req_cpu.to_string()),
            ("memory".to_string(), req_mem.to_string()),
        ])),
        limits: Some(HashMap::from([
            ("cpu".to_string(), lim_cpu.to_string()),
            ("memory".to_string(), lim_mem.to_string()),
        ])),
        claims: None,
    }
}

/// Build a `ResourceRequirements` with only requests set (no limits).
fn requests_only_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(HashMap::from([
            ("cpu".to_string(), cpu.to_string()),
            ("memory".to_string(), memory.to_string()),
        ])),
        limits: None,
        claims: None,
    }
}

/// Assemble a minimal `Pod` from a list of app containers.  `init_containers`
/// and `ephemeral_containers` are left `None` unless specified.
fn make_pod(
    name: &str,
    containers: Vec<Container>,
    init_containers: Option<Vec<Container>>,
    ephemeral_containers: Option<Vec<EphemeralContainer>>,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers,
            init_containers,
            ephemeral_containers,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: None,
            volumes: None,
            affinity: None,
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        }),
        status: None,
    }
}

// ---------------------------------------------------------------------------
// BestEffort cases
// ---------------------------------------------------------------------------

/// A pod with no spec at all is BestEffort.
///
/// Analogous to the empty-resources rows of upstream `TestGetPodQOS` in
/// `pkg/apis/core/v1/helper/qos/qos_test.go`.
#[test]
fn best_effort_no_spec() {
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("no-spec").with_namespace("default"),
        spec: None,
        status: None,
    };
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// Single container with no resource requirements → BestEffort.
///
/// Matches the "best-effort" rows of upstream `TestGetPodQOS`.
#[test]
fn best_effort_single_container_no_resources() {
    let c = make_container("c1");
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// Multiple containers, none with resource requirements → BestEffort.
///
/// Matches the "best-effort" rows of upstream `TestGetPodQOS`.
#[test]
fn best_effort_multiple_containers_no_resources() {
    let containers = vec![
        make_container("c1"),
        make_container("c2"),
        make_container("c3"),
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// Resource-less init containers leave a BestEffort pod BestEffort.
///
/// This is the one init-container scenario where Rusternetes and upstream
/// agree: when the init containers carry no resources there is nothing to fold
/// into the QoS calculation, so both classify the pod BestEffort.
#[test]
fn best_effort_with_init_containers_no_resources() {
    let app_containers = vec![make_container("app")];
    let init_containers = vec![make_container("init1"), make_container("init2")];
    let pod = make_pod("pod", app_containers, Some(init_containers), None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

// ---------------------------------------------------------------------------
// Guaranteed cases
// ---------------------------------------------------------------------------

/// Single container with matching CPU + memory limits == requests → Guaranteed.
///
/// Matches the "guaranteed" rows of upstream `TestGetPodQOS`.
#[test]
fn guaranteed_single_container_limits_eq_requests() {
    let c = with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi"));
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Multiple containers, all with matching limits == requests → Guaranteed.
///
/// Matches the "guaranteed" rows of upstream `TestGetPodQOS`.
#[test]
fn guaranteed_multiple_containers_all_matching() {
    let containers = vec![
        with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
        with_resources(make_container("c2"), guaranteed_resources("200m", "256Mi")),
        with_resources(make_container("c3"), guaranteed_resources("50m", "64Mi")),
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Guaranteed app containers plus Guaranteed init containers → Guaranteed.
///
/// `get_qos_class` folds the (also-Guaranteed) init container into the
/// calculation, matching upstream `ComputePodQOS`.
#[test]
fn guaranteed_app_containers_with_guaranteed_init_containers() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let init = vec![with_resources(
        make_container("init"),
        guaranteed_resources("50m", "64Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Ephemeral containers do NOT affect QoS class. A pod with Guaranteed app
/// containers stays Guaranteed even when an ephemeral (debug) container carries
/// mismatched resources.
///
/// This matches upstream: `GetPodQOS` in
/// `pkg/apis/core/v1/helper/qos/qos.go` iterates only regular and init
/// containers; ephemeral containers are excluded by design (they are also
/// forbidden from declaring resources in the API). Rusternetes likewise
/// excludes them.
#[test]
fn guaranteed_unaffected_by_ephemeral_containers() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    // Ephemeral container with resources set to something that would be Burstable
    let eph = EphemeralContainer {
        name: "debugger".to_string(),
        image: "debug-image:latest".to_string(),
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
        resources: Some(ResourceRequirements {
            requests: Some(HashMap::from([
                ("cpu".to_string(), "50m".to_string()),
                ("memory".to_string(), "64Mi".to_string()),
            ])),
            limits: Some(HashMap::from([
                ("cpu".to_string(), "200m".to_string()),
                ("memory".to_string(), "256Mi".to_string()),
            ])),
            claims: None,
        }),
        termination_message_path: None,
        termination_message_policy: None,
        ..Default::default()
    };
    let pod = make_pod("pod", app, None, Some(vec![eph]));
    assert_eq!(
        get_qos_class(&pod),
        QoSClass::Guaranteed,
        "ephemeral containers must not affect QoS class"
    );
}

// ---------------------------------------------------------------------------
// Burstable cases
// ---------------------------------------------------------------------------

/// requests < limits → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_requests_less_than_limits() {
    let c = with_resources(
        make_container("c1"),
        burstable_resources("100m", "128Mi", "200m", "256Mi"),
    );
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// requests only (no limits) → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_requests_only_no_limits() {
    let c = with_resources(
        make_container("c1"),
        requests_only_resources("100m", "128Mi"),
    );
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// A container with a single limits-only entry (cpu + memory limits, no
/// requests) is **Guaranteed**: `get_qos_class` defaults the missing request to
/// the matching limit before classifying, matching upstream.
///
/// Upstream reference: `ComputePodQOS` in
/// `pkg/apis/core/v1/helper/qos/qos.go`.
#[test]
fn limits_only_one_resource_is_guaranteed() {
    let c = with_resources(make_container("c1"), limits_only_resources("100m", "128Mi"));
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Upstream Kubernetes defaults a missing request to the matching limit before
/// classifying, so a limits-only container (cpu + memory limits) is
/// **Guaranteed**. `get_qos_class` mirrors that defaulting.
///
/// Mirrors: `ComputePodQOS` in `pkg/apis/core/v1/helper/qos/qos.go`.
#[test]
fn upstream_limits_only_should_be_guaranteed() {
    let c = with_resources(make_container("c1"), limits_only_resources("100m", "128Mi"));
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Mixed containers — one Guaranteed, one with no resources → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_mixed_guaranteed_and_best_effort_containers() {
    let containers = vec![
        with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
        make_container("c2"), // no resources — BestEffort
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// Partial CPU-only limit (no memory) → Burstable: Guaranteed requires both
/// cpu AND memory limits.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_cpu_only_limit_no_memory() {
    let r = ResourceRequirements {
        requests: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
        limits: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
        claims: None,
    };
    let c = with_resources(make_container("c1"), r);
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// Memory-only limit (no cpu) → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_memory_only_limit_no_cpu() {
    let r = ResourceRequirements {
        requests: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
        limits: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
        claims: None,
    };
    let c = with_resources(make_container("c1"), r);
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// One container Guaranteed, another with mismatched requests/limits → the
/// whole pod is Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_one_of_two_containers_not_guaranteed() {
    let containers = vec![
        with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
        with_resources(
            make_container("c2"),
            burstable_resources("50m", "64Mi", "100m", "128Mi"),
        ),
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

// ---------------------------------------------------------------------------
// Init-container contribution
//
// Upstream `ComputePodQOS` (pkg/apis/core/v1/helper/qos/qos.go) folds init
// containers into the QoS calculation alongside regular containers, and
// `get_qos_class` mirrors that: init containers participate in the
// classification.
// ---------------------------------------------------------------------------

/// A BestEffort app container with a Guaranteed init container is **Burstable**:
/// the init container contributes requests/limits, but the app container has
/// none, so the pod is not Guaranteed. Mirrors upstream.
#[test]
fn guaranteed_init_makes_best_effort_app_burstable() {
    let app = vec![make_container("app")]; // no resources
    let init = vec![with_resources(
        make_container("init"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// Upstream folds the Guaranteed init container into the calculation, so a
/// BestEffort app container plus a Guaranteed init container is **Burstable**
/// (the pod has some requests/limits overall but not on every container).
/// `get_qos_class` includes init containers and mirrors this.
#[test]
fn upstream_guaranteed_init_makes_best_effort_app_burstable() {
    let app = vec![make_container("app")]; // no resources
    let init = vec![with_resources(
        make_container("init"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// A Guaranteed app container with a Burstable init container is **Burstable**:
/// the init container's mismatched requests/limits downgrade the whole pod.
/// Mirrors upstream.
#[test]
fn burstable_init_downgrades_guaranteed_app_to_burstable() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let init = vec![with_resources(
        make_container("init"),
        burstable_resources("50m", "64Mi", "100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// Upstream folds the Burstable init container into the calculation, so a
/// Guaranteed app container plus a Burstable init container is **Burstable**.
/// `get_qos_class` includes init containers and mirrors this.
#[test]
fn upstream_burstable_init_downgrades_guaranteed_app_to_burstable() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let init = vec![with_resources(
        make_container("init"),
        burstable_resources("50m", "64Mi", "100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

// QoSClass ordering (BestEffort < Burstable < Guaranteed) is already covered
// by `test_qos_class_ordering` in `eviction_test.rs`; not duplicated here.

// ---------------------------------------------------------------------------
// Table-driven sweep
// ---------------------------------------------------------------------------

/// Table-driven sweep covering all three QoS classes with multiple container
/// configurations in a single pass. Structured like upstream's table-driven
/// `TestGetPodQOS`
/// (<https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/qos/qos_test.go>);
/// `get_qos_class` mirrors upstream, so the "limits only" row is Guaranteed.
#[test]
fn qos_classify_table() {
    struct Case {
        label: &'static str,
        containers: Vec<Container>,
        expected: QoSClass,
    }

    let cases = vec![
        Case {
            label: "no containers → BestEffort",
            containers: vec![],
            expected: QoSClass::BestEffort,
        },
        Case {
            label: "single container no resources → BestEffort",
            containers: vec![make_container("c")],
            expected: QoSClass::BestEffort,
        },
        Case {
            label: "two containers no resources → BestEffort",
            containers: vec![make_container("c1"), make_container("c2")],
            expected: QoSClass::BestEffort,
        },
        Case {
            label: "single container guaranteed → Guaranteed",
            containers: vec![with_resources(
                make_container("c"),
                guaranteed_resources("100m", "128Mi"),
            )],
            expected: QoSClass::Guaranteed,
        },
        Case {
            label: "two containers both guaranteed → Guaranteed",
            containers: vec![
                with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
                with_resources(make_container("c2"), guaranteed_resources("200m", "256Mi")),
            ],
            expected: QoSClass::Guaranteed,
        },
        Case {
            label: "requests only → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                requests_only_resources("100m", "128Mi"),
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "limits only → Guaranteed (missing requests default to limits)",
            containers: vec![with_resources(
                make_container("c"),
                limits_only_resources("100m", "128Mi"),
            )],
            expected: QoSClass::Guaranteed,
        },
        Case {
            label: "requests < limits → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                burstable_resources("100m", "128Mi", "200m", "256Mi"),
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "one guaranteed one no-resources → Burstable",
            containers: vec![
                with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
                make_container("c2"),
            ],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "cpu-only limit → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                ResourceRequirements {
                    requests: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
                    limits: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
                    claims: None,
                },
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "memory-only limit → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                ResourceRequirements {
                    requests: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
                    limits: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
                    claims: None,
                },
            )],
            expected: QoSClass::Burstable,
        },
    ];

    for case in cases {
        let pod = make_pod("pod", case.containers, None, None);
        assert_eq!(
            get_qos_class(&pod),
            case.expected,
            "case failed: {}",
            case.label
        );
    }
}

// ---------------------------------------------------------------------------
// Status field reflection
// ---------------------------------------------------------------------------

/// When a pod already has a `.status.qos_class` string set (written by the
/// kubelet after scheduling), the classification function still derives the
/// class from the spec, not from the cached status field.
#[test]
fn get_qos_class_ignores_status_field() {
    let c = with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi"));
    let mut pod = make_pod("pod", vec![c], None, None);
    // Deliberately set an incorrect status field.
    pod.status = Some(PodStatus {
        qos_class: Some("BestEffort".to_string()),
        ..Default::default()
    });
    // Classification must derive from spec, not status.
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}
